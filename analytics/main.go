package main

import (
	"log"
	"net/http"

	"distributed-rate-limiter/internal/config"
)

func main() {

	store := NewViolationStore(config.AnalyticsAlertThreshold, config.AnalyticsAlertWindow)

	subscriber, err := NewSubscriber(config.NatsURL, store)
	if err != nil {
		log.Fatal(err)
	}
	defer subscriber.Close()

	http.HandleFunc("/stats", statsHandler(store))

	log.Printf("Analytics listening on %s", config.AnalyticsPort)

	log.Fatal(http.ListenAndServe(config.AnalyticsPort, nil))
}
