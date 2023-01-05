package store

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"go.sadegh.io/expi/types"

	"github.com/gomodule/redigo/redis"
)

type DataStore struct {
	pool    *redis.Pool
	market  chan MarketEvent
	trading chan TradingEvent
}

func NewDataStore(pool *redis.Pool) *DataStore {
	return &DataStore{pool: pool, market: make(chan MarketEvent), trading: make(chan TradingEvent)}
}

func (s *DataStore) symbolsKey(exchange string) string {
	return fmt.Sprintf("S:%s", exchange)
}

func (s *DataStore) candlesKey(exchange string) string {
	return fmt.Sprintf("C:%s", exchange)
}

func (s *DataStore) SetCandles(candles types.Candles, id, exchange string) error {
	var buf bytes.Buffer
	var conn = s.pool.Get()
	defer func() { _ = conn.Close() }()

	err := gob.NewEncoder(&buf).Encode(candles)
	if err != nil {
		return err
	}

	_, err = conn.Do("HSET", s.candlesKey(exchange), id, buf.String())
	if err != nil {
		return err
	}

	return err
}

func (s *DataStore) GetCandles(id, exchange string) (types.Candles, error) {
	var candles types.Candles
	var conn = s.pool.Get()
	defer func() { _ = conn.Close() }()

	b, err := redis.Bytes(conn.Do("HGET", s.candlesKey(exchange), id))
	if err != nil {
		return candles, err
	}

	err = gob.NewDecoder(bytes.NewReader(b)).Decode(&candles)
	return candles, err
}

func (s *DataStore) SetSymbol(symbol types.Symbol, exchange string) error {
	var buf bytes.Buffer
	var conn = s.pool.Get()
	defer func() { _ = conn.Close() }()

	err := gob.NewEncoder(&buf).Encode(symbol)
	if err != nil {
		return err
	}

	_, err = conn.Do("HSET", s.symbolsKey(exchange), symbol.ID, buf.String())
	if err != nil {
		return err
	}

	return err
}

func (s *DataStore) SetSymbols(symbols types.Symbols, exchange string) error {
	var conn = s.pool.Get()
	defer func() { _ = conn.Close() }()

	_ = conn.Send("MULTI")

	for _, symbol := range symbols {
		var buf bytes.Buffer

		err := gob.NewEncoder(&buf).Encode(symbol)
		if err != nil {
			return err
		}

		err = conn.Send("HSET", s.symbolsKey(exchange), symbol.ID, buf.String())
		if err != nil {
			return err
		}
	}

	_, err := conn.Do("EXEC")
	return err
}

func (s *DataStore) GetSymbol(id, exchange string) (types.Symbol, error) {
	var symbol types.Symbol
	var conn = s.pool.Get()
	defer func() { _ = conn.Close() }()

	b, err := redis.Bytes(conn.Do("HGET", s.symbolsKey(exchange), id))
	if err != nil {
		return symbol, err
	}

	err = gob.NewDecoder(bytes.NewReader(b)).Decode(&symbol)
	return symbol, err
}

func (s *DataStore) GetSymbols(exchange string) (types.Symbols, error) {
	var symbols types.Symbols
	var conn = s.pool.Get()
	defer func() { _ = conn.Close() }()

	b, err := redis.ByteSlices(conn.Do("HVALS", s.symbolsKey(exchange)))
	if err != nil {
		return symbols, err
	}

	for _, d := range b {
		var symbol types.Symbol
		err = gob.NewDecoder(bytes.NewReader(d)).Decode(&symbol)
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
	conn := &redis.PubSubConn{Conn: s.pool.Get()}
	if err := conn.Subscribe(marketEvents, tradingEvents); err != nil {
		return err
	}

	for conn.Conn.Err() == nil {
		switch e := conn.Receive().(type) {
		case redis.Message:
			switch e.Channel {
			case marketEvents:
				var event MarketEvent
				_ = gob.NewDecoder(bytes.NewReader(e.Data)).Decode(&event)
				s.market <- event
			case tradingEvents:
				var event TradingEvent
				_ = gob.NewDecoder(bytes.NewReader(e.Data)).Decode(&event)
				s.trading <- event
			}
		}
	}

	return conn.Conn.Close()
}

func (s *DataStore) AnnounceReport(event TradingEvent) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(event)
	if err != nil {
		return err
	}

	_, err = s.pool.Get().Do("PUBLISH", tradingEvents, buf.String())
	return err
}

func (s *DataStore) AnnounceEvent(event MarketEvent) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(event)
	if err != nil {
		return err
	}

	_, err = s.pool.Get().Do("PUBLISH", marketEvents, buf.String())
	return err
}
