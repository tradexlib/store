package store

import "go.sadegh.io/expi/types"

type MarketEvent struct {
	Symbol   string
	Exchange string
}

type TradingEvent struct {
	Exchange string
	UserID   string
	Report   types.Report
}

type KeyEvent struct {
	UserID    string
	PublicKey string
	SecretKey string
}
