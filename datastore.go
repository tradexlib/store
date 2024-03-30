package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.sadegh.io/expi/types"
)

type DataStore struct {
	client  *redis.Client
	market  chan MarketEvent
	trading chan TradingEvent
}

func NewDataStore(client *redis.Client) *DataStore {
	return &DataStore{client: client, market: make(chan MarketEvent), trading: make(chan TradingEvent)}
}

func (s *DataStore) symbolsKey(exchange string) string {
	return fmt.Sprintf("S:%s", exchange)
}

func (s *DataStore) candlesKey(exchange string) string {
	return fmt.Sprintf("C:%s", exchange)
}

func (s *DataStore) changeHistoryKey(id, exchange string) string {
	return fmt.Sprintf("CH:%s:%s", id, exchange)
}

func (s *DataStore) SetChangeHistory(id, exchange string) error {
	return s.client.Set(context.TODO(), s.changeHistoryKey(id, exchange), time.Now().Format(time.RFC3339), -1).Err()
}

func (s *DataStore) GetChangeHistory(id, exchange string) (time.Time, error) {
	history := s.client.Get(context.TODO(), s.changeHistoryKey(id, exchange)).String()
	return time.Parse(time.RFC3339, history)
}

func (s *DataStore) SetCandles(candles types.Candles, id, exchange string) error {
	var buf bytes.Buffer

	err := gob.NewEncoder(&buf).Encode(candles)
	if err != nil {
		return err
	}

	return s.client.HSet(context.TODO(), s.candlesKey(exchange), id, buf.String()).Err()
}

func (s *DataStore) GetCandles(id, exchange string) (types.Candles, error) {
	var candles types.Candles
	b, err := s.client.HGet(context.TODO(), s.candlesKey(exchange), id).Bytes()
	if err != nil {
		return candles, err
	}

	err = gob.NewDecoder(bytes.NewReader(b)).Decode(&candles)
	return candles, err
}

func (s *DataStore) GetPrice(id, exchange string) float64 {
	candles, _ := s.GetCandles(id, exchange)
	if len(candles) > 0 {
		return candles[0].Close
	}

	return 0
}

func (s *DataStore) SetSymbol(symbol types.Symbol, exchange string) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(symbol)
	if err != nil {
		return err
	}

	return s.client.HSet(context.TODO(), s.symbolsKey(exchange), symbol.ID, buf.String()).Err()
}

func (s *DataStore) SetSymbols(symbols types.Symbols, exchange string) error {
	for _, symbol := range symbols {
		var buf bytes.Buffer

		err := gob.NewEncoder(&buf).Encode(symbol)
		if err != nil {
			return err
		}

		err = s.client.HSet(context.TODO(), s.symbolsKey(exchange), symbol.ID, buf.String()).Err()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *DataStore) GetSymbol(id, exchange string) (types.Symbol, error) {
	var symbol types.Symbol

	b, err := s.client.HGet(context.TODO(), s.symbolsKey(exchange), id).Bytes()
	if err != nil {
		return symbol, err
	}

	err = gob.NewDecoder(bytes.NewReader(b)).Decode(&symbol)
	return symbol, err
}

func (s *DataStore) GetSymbols(exchange string) (types.Symbols, error) {
	vals := s.client.HVals(context.TODO(), s.symbolsKey(exchange)).Val()

	var err error
	var symbols types.Symbols
	for _, val := range vals {
		var symbol types.Symbol
		buffer := bytes.NewBuffer([]byte(val))
		err = gob.NewDecoder(buffer).Decode(&symbol)
		symbols = append(symbols, symbol)
	}

	return symbols, err
}

func (s *DataStore) MarketChan() chan MarketEvent {
	return s.market
}

func (s *DataStore) TradingChan() chan TradingEvent {
	return s.trading
}

func (s *DataStore) SubscribeEvents() error {
	sub := s.client.Subscribe(context.TODO(), marketEvents, tradingEvents)
	for {
		event, err := sub.ReceiveMessage(context.TODO())
		if err != nil {
			continue
		}
		buffer := bytes.NewBuffer([]byte(event.Payload))
		switch event.Channel {
		case marketEvents:
			var marketEvent MarketEvent
			_ = gob.NewDecoder(buffer).Decode(&marketEvent)
			s.market <- marketEvent
		case tradingEvents:
			var tradingEvent TradingEvent
			_ = gob.NewDecoder(buffer).Decode(&tradingEvent)
			s.trading <- tradingEvent
		}
	}
}

func (s *DataStore) AnnounceReport(event TradingEvent) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(event)
	if err != nil {
		return err
	}

	return s.client.Publish(context.TODO(), tradingEvents, buf.String()).Err()
}

func (s *DataStore) AnnounceEvent(event MarketEvent) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(event)
	if err != nil {
		return err
	}

	return s.client.Publish(context.TODO(), marketEvents, buf.String()).Err()
}
