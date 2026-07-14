package main

import (
	"encoding/json"
	"log"
	"time"

	"distributed-rate-limiter/internal/events"

	"github.com/nats-io/nats.go"
)

// Subscriber consumes violation events published by the edge over NATS and
// feeds them into the ViolationStore. This is the receiving side of the
// Asynchronous Event-Driven Messaging pattern: analytics is fully decoupled
// from the edge, it only needs the broker to be reachable.
type Subscriber struct {
	nc  *nats.Conn
	sub *nats.Subscription
}

func NewSubscriber(natsURL string, store *ViolationStore) (*Subscriber, error) {
	// See the matching comment in edge/violation_publisher.go: this makes the
	// initial connection resilient to start-up ordering in Docker Compose.
	nc, err := nats.Connect(
		natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, err
	}

	sub, err := nc.Subscribe(events.ViolationsSubject, func(msg *nats.Msg) {
		var ev events.ViolationEvent
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			log.Printf("subscriber: invalid violation event: %v", err)
			return
		}
		store.Record(ev.ClientID)
	})
	if err != nil {
		nc.Close()
		return nil, err
	}

	return &Subscriber{nc: nc, sub: sub}, nil
}

func (s *Subscriber) Close() {
	_ = s.sub.Unsubscribe()
	s.nc.Drain()
}
