package main

import (
	"encoding/json"
	"net/http"
)

// statsHandler espone i conteggi attuali dei rifiuti
// per ciascun client relativi alla finestra attiva.
func statsHandler(store *ViolationStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(store.Snapshot())
	}
}
