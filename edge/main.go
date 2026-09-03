package main

import (
	"context"
	"log"
	"net/http"

	"distributed-rate-limiter/internal/config"
	"distributed-rate-limiter/internal/tracing"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

func main() {

	shutdown, err := tracing.Init(context.Background(), "edge")
	if err != nil {
		log.Fatal(err)
	}
	defer shutdown(context.Background())

	aggregatorClient, err := NewAggregatorClient()
	if err != nil {
		log.Fatal(err)
	}

	publisher, err := NewViolationPublisher(config.NatsURL)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("/api", apiHandler(aggregatorClient, publisher))

	log.Printf("Edge listening on %s", config.EdgePort)

	log.Fatal(http.ListenAndServe(config.EdgePort, otelhttp.NewHandler(mux, "edge")))

}
