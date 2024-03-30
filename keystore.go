package store

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type KeyStore struct {
	client *redis.Client
	events chan KeyEvent
}

func NewKeyStore(client *redis.Client) *KeyStore {
	return &KeyStore{client: client, events: make(chan KeyEvent)}
}

func (s *KeyStore) exchangeKey(uid string) string {
	return fmt.Sprintf("KEYS:%s", uid)
}

func (s *KeyStore) EventsChan() chan KeyEvent {
	return s.events
}

func (s *KeyStore) SubscribeEvents() error {
	sub := s.client.Subscribe(context.TODO(), keyStoreEvents)
	for {
		event, err := sub.ReceiveMessage(context.TODO())
		if err != nil {
			continue
		}
		buffer := bytes.NewBuffer([]byte(event.Payload))
		switch event.Channel {
		case keyStoreEvents:
			var keyEvent KeyEvent
			_ = gob.NewDecoder(buffer).Decode(&keyEvent)
			_ = s.setKeys(keyEvent)
			s.events <- keyEvent
		}
	}
}

func (s *KeyStore) setKeys(event KeyEvent) error {
	var buf bytes.Buffer

	err := gob.NewEncoder(&buf).Encode(event)
	if err != nil {
		return err
	}

	return s.client.HSet(context.TODO(), s.exchangeKey(event.UserID), event.Exchange, buf.String()).Err()
}

func (s *KeyStore) GetKeys(uid, exchange string) (string, string) {
	var event KeyEvent
	b, _ := s.client.HGet(context.TODO(), s.exchangeKey(uid), exchange).Bytes()
	_ = gob.NewDecoder(bytes.NewReader(b)).Decode(&event)

	return event.PublicKey, event.SecretKey
}

func (s *KeyStore) UpdateKeys(event KeyEvent) error {
	var buf bytes.Buffer
	err := gob.NewEncoder(&buf).Encode(event)
	if err != nil {
		return err
	}

	return s.client.Publish(context.TODO(), keyStoreEvents, buf.String()).Err()
}
