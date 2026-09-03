package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"distributed-rate-limiter/internal/events"

	"github.com/nats-io/nats.go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

var tracer = otel.Tracer("analytics")

// qui si acquisiscono gli eventi di violazione pubblicati dall'edge
// tramite NATS e si inseriscono nel ViolationStore.
type Subscriber struct {
	nc  *nats.Conn
	sub *nats.Subscription
}

func NewSubscriber(natsURL string, store *ViolationStore) (*Subscriber, error) {
	nc, err := nats.Connect(
		natsURL,
		nats.RetryOnFailedConnect(true), //per essere resiliente all'avvio
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

		carrier := propagation.MapCarrier{"traceparent": ev.TraceParent}
		ctx := otel.GetTextMapPropagator().Extract(context.Background(), carrier)
		_, span := tracer.Start(ctx, "analytics.ProcessViolation")
		store.Record(ev.ClientID)
		span.End()
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
