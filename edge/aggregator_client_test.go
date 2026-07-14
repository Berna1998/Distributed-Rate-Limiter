package main

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	pb "distributed-rate-limiter/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// fakeAggregator is a minimal RateLimiterServer whose CheckQuota can be
// flipped to fail on demand, so tests can simulate a node going down without
// touching real network sockets or the real bucket/gossip logic.
type fakeAggregator struct {
	pb.UnimplementedRateLimiterServer
	failing atomic.Bool
	calls   atomic.Int32
}

func (f *fakeAggregator) CheckQuota(_ context.Context, _ *pb.QuotaRequest) (*pb.QuotaResponse, error) {
	f.calls.Add(1)
	if f.failing.Load() {
		return nil, errors.New("simulated aggregator failure")
	}
	return &pb.QuotaResponse{Allowed: true, RemainingTokens: 4}, nil
}

func startFakeAggregator(t *testing.T) (*fakeAggregator, *bufconn.Listener) {
	t.Helper()

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	fake := &fakeAggregator{}
	pb.RegisterRateLimiterServer(srv, fake)

	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	return fake, lis
}

func dialBufconn(t *testing.T, lis *bufconn.Listener) pb.RateLimiterClient {
	t.Helper()

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial bufconn: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	return pb.NewRateLimiterClient(conn)
}

func TestAggregatorClient_FailsOverToHealthyNode(t *testing.T) {
	nodeA, lisA := startFakeAggregator(t)
	nodeB, lisB := startFakeAggregator(t)
	nodeA.failing.Store(true)

	client := &AggregatorClient{
		clients:   []pb.RateLimiterClient{dialBufconn(t, lisA), dialBufconn(t, lisB)},
		addresses: []string{"nodeA", "nodeB"},
		breakers: []*CircuitBreaker{
			NewCircuitBreaker(2, 5*time.Second),
			NewCircuitBreaker(2, 5*time.Second),
		},
	}

	const clientID = "any-client"

	// two failed attempts trip node A's breaker (threshold=2)
	for i := 0; i < 2; i++ {
		if _, _, err := client.CheckQuota(clientID); err != nil {
			t.Fatalf("unexpected error on attempt %d: %v", i+1, err)
		}
	}
	callsOnAAfterOpen := nodeA.calls.Load()

	for i := 0; i < 3; i++ {
		if _, _, err := client.CheckQuota(clientID); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if nodeA.calls.Load() != callsOnAAfterOpen {
		t.Fatalf("expected no further attempts against the open-circuit node, got %d new calls",
			nodeA.calls.Load()-callsOnAAfterOpen)
	}
	if nodeB.calls.Load() == 0 {
		t.Fatal("expected the healthy node to receive the failover traffic")
	}
}

func TestAggregatorClient_DegradedWhenAllNodesDown(t *testing.T) {
	nodeA, lisA := startFakeAggregator(t)
	nodeB, lisB := startFakeAggregator(t)
	nodeA.failing.Store(true)
	nodeB.failing.Store(true)

	client := &AggregatorClient{
		clients:   []pb.RateLimiterClient{dialBufconn(t, lisA), dialBufconn(t, lisB)},
		addresses: []string{"nodeA", "nodeB"},
		breakers: []*CircuitBreaker{
			NewCircuitBreaker(2, 5*time.Second),
			NewCircuitBreaker(2, 5*time.Second),
		},
	}

	resp, degraded, err := client.CheckQuota("any-client")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !degraded {
		t.Fatal("expected degraded=true when every aggregator is unavailable")
	}
	if !resp.Allowed {
		t.Fatal("expected fail-open behavior: request should be allowed while degraded")
	}
}
