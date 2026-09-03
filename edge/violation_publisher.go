package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"distributed-rate-limiter/internal/config"
	"distributed-rate-limiter/internal/events"

	"github.com/nats-io/nats.go"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

// ViolationPublisher inoltra gli eventi relativi alle richieste rifiutate al servizio di analisi
// tramite NATS senza mai bloccare il percorso della richiesta verso il client.
type ViolationPublisher struct {
	nc     *nats.Conn
	events chan events.ViolationEvent
}

func NewViolationPublisher(natsURL string) (*ViolationPublisher, error) {
	nc, err := nats.Connect(
		natsURL,
		nats.RetryOnFailedConnect(true), //per essere resiliente all'avvio
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

// inserisce in coda un evento di violazione senza bloccare il chiamante. Se la
// coda interna è piena, l'evento viene scartato e registrato. ctx porta il
// contesto di tracing della richiesta HTTP corrente, iniettato nell'evento
// così la traccia può proseguire dentro analytics.
func (p *ViolationPublisher) Publish(ctx context.Context, clientID string) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	ev := events.ViolationEvent{
		ClientID:    clientID,
		Timestamp:   time.Now(),
		TraceParent: carrier.Get("traceparent"),
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
