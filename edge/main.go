package main

import (
	"log"
	"net/http"

	"distributed-rate-limiter/internal/config"
)

func main() {

	aggregatorClient, err := NewAggregatorClient()
	if err != nil {
		log.Fatal(err)
	}

	publisher, err := NewViolationPublisher(config.NatsURL)
	if err != nil {
		log.Fatal(err)
	}
	defer publisher.Close()

	http.HandleFunc("/api", apiHandler(aggregatorClient, publisher))

	log.Printf("Edge listening on %s", config.EdgePort)

	log.Fatal(http.ListenAndServe(config.EdgePort, nil))

}
