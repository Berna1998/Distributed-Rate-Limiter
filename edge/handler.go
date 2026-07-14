package main

import (
	"log"
	"net/http"
)

func apiHandler(client *AggregatorClient, publisher *ViolationPublisher) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		clientID := r.Header.Get("X-Client-ID")

		if clientID == "" {
			http.Error(w, "missing client id", http.StatusBadRequest)
			return
		}

		resp, degraded, err := client.CheckQuota(clientID)

		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if degraded {
			w.Header().Set("X-RateLimit-Degraded", "true")
		}

		log.Printf("Remaining tokens: %d", resp.RemainingTokens)

		if resp.Allowed {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("Request allowed"))
			return
		}

		publisher.Publish(clientID)
		http.Error(w, "Rate limit exceeded", http.StatusTooManyRequests)

	}
}
