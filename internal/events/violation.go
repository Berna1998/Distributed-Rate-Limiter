package events

import "time"

// ViolationsSubject è il soggetto NATS a cui l'edge pubblica gli eventi “rejected-request”
// e a cui Analytics si abbona.
const ViolationsSubject = "ratelimiter.violations"

type ViolationEvent struct {
	ClientID  string    `json:"client_id"`
	Timestamp time.Time `json:"timestamp"`

	// TraceParent porta il contesto di tracing W3C (se presente) dall'edge
	// fino ad analytics, così la traccia distribuita attraversa anche il
	// confine asincrono di NATS invece di fermarsi all'edge.
	TraceParent string `json:"traceparent,omitempty"`
}
