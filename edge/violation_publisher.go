package main

import (
	"encoding/json"
	"log"
	"time"

	"distributed-rate-limiter/internal/config"
	"distributed-rate-limiter/internal/events"

	"github.com/nats-io/nats.go"
)

// ViolationPublisher forwards rejected-request events to the analytics
// service over NATS without ever blocking the client-facing request path.
// A bounded queue absorbs bursts; if it fills up (broker slow/down) events
// are dropped rather than piling up goroutines or adding latency — under a
// real request flood, the logging/alerting path must not become a second
// bottleneck on top of the one it's supposed to be observing.
type ViolationPublisher struct {
	nc     *nats.Conn
	events chan events.ViolationEvent
}

func NewViolationPublisher(natsURL string) (*ViolationPublisher, error) {
	// RetryOnFailedConnect makes the initial dial resilient to start-up
	// ordering (e.g. in Docker Compose, if NATS isn't accepting connections
	// yet): instead of failing immediately, the client keeps retrying in the
	// background and buffers publishes until it connects.
	nc, err := nats.Connect(
		natsURL,
		nats.RetryOnFailedConnect(true),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(2*time.Second),
	)
	if err != nil {
		return nil, err
	}

	p := &ViolationPublisher{
		nc:     nc,
		events: make(chan events.ViolationEvent, config.ViolationQueueSize),
	}

	go p.run()

	return p, nil
}

func (p *ViolationPublisher) run() {
	for ev := range p.events {
		payload, err := json.Marshal(ev)
		if err != nil {
			log.Printf("violation publisher: marshal error: %v", err)
			continue
		}
		if err := p.nc.Publish(events.ViolationsSubject, payload); err != nil {
			log.Printf("violation publisher: publish error: %v", err)
		}
	}
}

// Publish enqueues a violation event without blocking the caller. If the
// internal queue is full, the event is dropped and logged rather than
// applying back-pressure to the HTTP request path.
func (p *ViolationPublisher) Publish(clientID string) {
	ev := events.ViolationEvent{
		ClientID:  clientID,
		Timestamp: time.Now(),
	}

	select {
	case p.events <- ev:
	default:
		log.Printf("violation publisher: queue full, dropping event for client %s", clientID)
	}
}

func (p *ViolationPublisher) Close() {
	close(p.events)
	p.nc.Drain()
}
