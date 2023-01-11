package store

import (
	"bytes"
	"encoding/gob"
	"fmt"

	"github.com/gomodule/redigo/redis"
)

type KeyStore struct {
	pool   *redis.Pool
	events chan KeyEvent
}

func NewKeyStore(pool *redis.Pool) *KeyStore {
	return &KeyStore{pool: pool, events: make(chan KeyEvent)}
}

func (s *KeyStore) exchangeKey(uid string) string {
	return fmt.Sprintf("KEYS:%s", uid)
}

func (s *KeyStore) EventsChan() chan KeyEvent {
	return s.events
}

func (s *KeyStore) SubscribeEvents() error {
	conn := &redis.PubSubConn{Conn: s.pool.Get()}
	if err := conn.Subscribe(keyStoreEvents); err != nil {
		return err
	}

	for conn.Conn.Err() == nil {
		switch e := conn.Receive().(type) {
		case redis.Message:
			switch e.Channel {
			case keyStoreEvents:
				var event KeyEvent
				_ = gob.NewDecoder(bytes.NewReader(e.Data)).Decode(&event)
				_ = s.setKeys(event)
				s.events <- event
			}
		}
	}

	return conn.Conn.Close()
}

func (s *KeyStore) setKeys(event KeyEvent) error {
	var buf bytes.Buffer
	var conn = s.pool.Get()
	defer func() { _ = conn.Close() }()

	err := gob.NewEncoder(&buf).Encode(event)
	if err != nil {
		return err
	}

	_, err = conn.Do("HSET", s.exchangeKey(event.UserID), event.Exchange, buf.String())
	if err != nil {
		return err
	}

	return err
}

func (s *KeyStore) GetKeys(uid, exchange string) (string, string) {
	var conn = s.pool.Get()
	var event KeyEvent
	defer func() { _ = conn.Close() }()

	b, _ := redis.Bytes(conn.Do("HGET", s.exchangeKey(uid), exchange))
	_ = gob.NewDecoder(bytes.NewReader(b)).Decode(&event)

	return event.PublicKey, event.SecretKey
}

func (s *KeyStore) UpdateKeys(event KeyEvent) error {
	var conn = s.pool.Get()
	defer func() { _ = conn.Close() }()

	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(event)
	if err != nil {
		return err
	}

	_, err = conn.Do("PUBLISH", keyStoreEvents, buf.String())
	return err
}
