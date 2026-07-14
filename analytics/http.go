package main

import (
	"encoding/json"
	"net/http"
)

// statsHandler exposes the current per-client rejection counts for the
// active window, mainly so the scalability test scenario can inspect
// alerting accuracy without grepping logs.
func statsHandler(store *ViolationStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(store.Snapshot())
	}
}
