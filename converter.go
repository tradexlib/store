package store

import "go.sadegh.io/expi/types"

const (
	USD = "USDT"
	BTC = "BTC"
	ETH = "ETH"
)

var currencies = []string{USD, BTC, ETH}

func (s *DataStore) ConvertSymbol(currency, exchange string) (symbol types.Symbol) {
	if len(currency) >= 6 {
		symbol, _ = s.GetSymbol(currency, exchange)
		if symbol.ID == "" {
			return
		}

		return
	}

	if contains(currencies, currency) {
		symbol.Base = currency
		symbol.Quote = USD

		if symbol.Base == USD {
			symbol.Base = BTC
		}

		symbol.ID = symbol.Base + symbol.Quote

		return
	}

	for _, base := range currencies {
		symbol, _ = s.GetSymbol(currency+base, exchange)
		if symbol.ID != "" {
			return
		}
	}

	return
}

func (s *DataStore) ConvertUSD(id, exchange string, value float64) (result float64) {
	switch {
	case value == 0 || id == USD:
		return value
	case id == BTC || id == ETH:
		BaseUsd := s.GetPrice(id+USD, exchange)

		return value * BaseUsd
	}

	symbol := s.ConvertSymbol(id, exchange)
	if symbol.ID == "" {
		return
	}

	switch symbol.Quote {
	case USD:
		BaseUsd := s.GetPrice(symbol.Base+USD, exchange)

		return value * BaseUsd
	case BTC:
		BaseBtc := s.GetPrice(symbol.Base+BTC, exchange)
		BtcUsd := s.GetPrice(BTC+USD, exchange)

		return value * BtcUsd * BaseBtc
	case ETH:
		BaseEth := s.GetPrice(symbol.Base+ETH, exchange)
		EthUsd := s.GetPrice(ETH+USD, exchange)

		return value * EthUsd * BaseEth
	default:
		return value
	}
}
