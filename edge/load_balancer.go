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

	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
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
			grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
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

// associo in modo deterministico un client_id a un singolo nodo aggregatore,
func (a *AggregatorClient) primaryIndex(clientID string) int {
	h := fnv.New32a()
	h.Write([]byte(clientID))
	return int(h.Sum32() % uint32(len(a.clients)))
}

// parte dal nodo primario del client e percorre i restanti
// nodi in ordine ad anello, in modo che un failover ricada sempre su un nodo che possiede (o che
// avrà a breve, tramite gossip) una replica dello stato del bucket del client.
func (a *AggregatorClient) candidateOrder(clientID string) []int {
	n := len(a.clients)
	primary := a.primaryIndex(clientID)

	order := make([]int, n)
	for i := 0; i < n; i++ {
		order[i] = (primary + i) % n
	}
	return order
}

// prova prima l'aggregatore primario del client, poi in caso
// agli altri nodi se il circuito aperto o se chiamata fallisce. ctx viene
// propagato al gRPC (deve derivare dalla richiesta HTTP in ingresso) così la
// traccia distribuita continua dentro l'aggregator invece di ripartire da zero.
func (a *AggregatorClient) CheckQuota(ctx context.Context, clientID string) (resp *pb.QuotaResponse, degraded bool, err error) {

	for _, idx := range a.candidateOrder(clientID) {
		breaker := a.breakers[idx]

		if !breaker.Allow() {
			log.Printf("Circuit open for Aggregator #%d (%s), skipping", idx+1, a.addresses[idx])
			continue
		}

		callCtx, cancel := context.WithTimeout(ctx, time.Second)
		resp, err = a.clients[idx].CheckQuota(callCtx, &pb.QuotaRequest{ClientId: clientID})
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
