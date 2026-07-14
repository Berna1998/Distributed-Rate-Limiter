package events

import "time"

// ViolationsSubject is the NATS subject the edge publishes rejected-request
// events to, and analytics subscribes to. JSON (not protobuf) is used
// deliberately here to keep this async, decoupled path visibly distinct from
// the synchronous gRPC/Protobuf control path between edge and aggregator.
const ViolationsSubject = "ratelimiter.violations"

type ViolationEvent struct {
	ClientID  string    `json:"client_id"`
	Timestamp time.Time `json:"timestamp"`
}
