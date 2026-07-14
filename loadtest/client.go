package main

import (
	"io"
	"net/http"
	"time"
)

// RequestResult captures everything a scenario needs to know about a single
// call to the edge: whether it was allowed, how long it took, and whether it
// was served in degraded (fail-open) mode.
type RequestResult struct {
	ClientID   string
	StatusCode int
	Allowed    bool
	Degraded   bool
	LatencyMs  float64
	Err        error
}

func callEdge(edgeURL, clientID string) RequestResult {
	req, err := http.NewRequest(http.MethodGet, edgeURL, nil)
	if err != nil {
		return RequestResult{ClientID: clientID, Err: err}
	}
	req.Header.Set("X-Client-ID", clientID)

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	latencyMs := time.Since(start).Seconds() * 1000

	if err != nil {
		return RequestResult{ClientID: clientID, LatencyMs: latencyMs, Err: err}
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	return RequestResult{
		ClientID:   clientID,
		StatusCode: resp.StatusCode,
		Allowed:    resp.StatusCode == http.StatusOK,
		Degraded:   resp.Header.Get("X-RateLimit-Degraded") == "true",
		LatencyMs:  latencyMs,
	}
}
