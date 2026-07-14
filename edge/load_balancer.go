package main

import (
	"context"
	"hash/fnv"
	"log"
	"time"

	"distributed-rate-limiter/internal/config"
	pb "distributed-rate-limiter/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type AggregatorClient struct {
	clients   []pb.RateLimiterClient
	addresses []string
	breakers  []*CircuitBreaker
}

func NewAggregatorClient() (*AggregatorClient, error) {

	var clients []pb.RateLimiterClient
	var breakers []*CircuitBreaker

	for _, address := range config.AggregatorAddresses {

		conn, err := grpc.NewClient(
			address,
			grpc.WithTransportCredentials(
				insecure.NewCredentials(),
			),
		)

		if err != nil {
			return nil, err
		}

		clients = append(
			clients,
			pb.NewRateLimiterClient(conn),
		)
		breakers = append(
			breakers,
			NewCircuitBreaker(config.CircuitBreakerThreshold, config.CircuitBreakerCooldown),
		)
	}

	return &AggregatorClient{
		clients:   clients,
		addresses: config.AggregatorAddresses,
		breakers:  breakers,
	}, nil
}

// primaryIndex maps a client_id deterministically to a single aggregator node,
// so a client's bucket state normally lives on one node and gossip only needs
// to cover replication/failover instead of being the sole consistency mechanism.
func (a *AggregatorClient) primaryIndex(clientID string) int {
	h := fnv.New32a()
	h.Write([]byte(clientID))
	return int(h.Sum32() % uint32(len(a.clients)))
}

// candidateOrder starts at the client's primary node and walks the remaining
// nodes in ring order, so a failover always lands on a node that has (or will
// soon have, via gossip) a replica of the client's bucket state.
func (a *AggregatorClient) candidateOrder(clientID string) []int {
	n := len(a.clients)
	primary := a.primaryIndex(clientID)

	order := make([]int, n)
	for i := 0; i < n; i++ {
		order[i] = (primary + i) % n
	}
	return order
}

// CheckQuota tries the client's primary aggregator first, then falls back to
// the other nodes in ring order if the primary's circuit is open or the call
// fails. If every node is unavailable, it fails open (degraded=true) rather
// than blocking legitimate traffic during a full aggregator outage.
func (a *AggregatorClient) CheckQuota(clientID string) (resp *pb.QuotaResponse, degraded bool, err error) {

	for _, idx := range a.candidateOrder(clientID) {
		breaker := a.breakers[idx]

		if !breaker.Allow() {
			log.Printf("Circuit open for Aggregator #%d (%s), skipping", idx+1, a.addresses[idx])
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err = a.clients[idx].CheckQuota(ctx, &pb.QuotaRequest{ClientId: clientID})
		cancel()

		if err != nil {
			log.Printf("Aggregator #%d (%s) failed: %v", idx+1, a.addresses[idx], err)
			breaker.RecordFailure()
			continue
		}

		breaker.RecordSuccess()
		return resp, false, nil
	}

	log.Printf("All aggregators unavailable for client %s, falling back to degraded fail-open", clientID)
	return &pb.QuotaResponse{Allowed: true, RemainingTokens: -1}, true, nil
}
