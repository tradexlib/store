package store

import (
	"bytes"
	"encoding/gob"

	"github.com/gomodule/redigo/redis"
)

type KeyStore struct {
	pool   *redis.Pool
	events chan KeyEvent
}

func NewKeyStore(pool *redis.Pool) *KeyStore {
	return &KeyStore{pool: pool, events: make(chan KeyEvent)}
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
				s.events <- event
			}
		}
	}

	return conn.Conn.Close()
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
