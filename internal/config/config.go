package config

import (
	"os"
	"strings"
	"time"
)

const (
	// Token Bucket
	DefaultCapacity   = 5.0
	DefaultRefillRate = 0.5 // tokens per second (1 token every 2 seconds)

	// Network
	EdgePort      = ":8081"
	AnalyticsPort = ":8082"

	// Circuit Breaker
	CircuitBreakerThreshold = 3
	CircuitBreakerCooldown  = 5 * time.Second

	// Gossip
	GossipInterval = 5 * time.Second

	// Violation messaging (edge -> analytics)
	ViolationQueueSize = 1024

	// Analytics alerting
	AnalyticsAlertThreshold = 5
	AnalyticsAlertWindow    = 60 * time.Second
)

var AggregatorAddresses = GetEnvList("AGGREGATOR_ADDRESSES", []string{
	"localhost:50051",
	"localhost:50052",
	"localhost:50053",
})

var NatsURL = GetEnv("NATS_URL", "nats://localhost:4222")

var OtelExporterEndpoint = GetEnv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317")

func GetEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func GetEnvList(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return strings.Split(v, ",")
}
