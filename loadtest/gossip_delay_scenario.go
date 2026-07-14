package main

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"distributed-rate-limiter/internal/config"
	pb "distributed-rate-limiter/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// runGossipDelayScenario measures how long gossip takes to propagate a
// bucket change from a client's primary node to the secondary. It talks gRPC
// directly to each aggregator (bypassing the edge and its routing).
//
// CheckQuota is the only RPC available and it mutates state, so polling the
// secondary repeatedly would interfere with itself (the merge is
// last-write-wins on a timestamp, and our own poll would keep bumping it).
// Instead each wait offset uses a fresh, never-seen client_id: drain the
// primary down to empty, wait, then take exactly one reading from the
// secondary. If gossip has already delivered the empty state, that single
// read comes back Allowed=false; if not, the secondary still has its own
// fresh bucket and returns Allowed=true. That's an unambiguous per-trial
// signal without needing a read-only "peek" RPC.
func runGossipDelayScenario(_ Config) {
	addresses := config.AggregatorAddresses
	if len(addresses) < 2 {
		fmt.Println("[gossip-delay] needs at least 2 aggregator addresses in config.AggregatorAddresses")
		return
	}

	clients := make(map[string]pb.RateLimiterClient, len(addresses))
	for _, addr := range addresses {
		conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			fmt.Printf("[gossip-delay] cannot dial %s: %v\n", addr, err)
			return
		}
		clients[addr] = pb.NewRateLimiterClient(conn)
	}

	waits := []time.Duration{
		0, 500 * time.Millisecond, 1 * time.Second, 1500 * time.Millisecond,
		2 * time.Second, 3 * time.Second, 4 * time.Second, 5 * time.Second,
		6 * time.Second, 7 * time.Second,
	}
	capacity := int(config.DefaultCapacity)

	writer, err := NewResultWriter("loadtest/results/gossip_delay.csv",
		[]string{"wait_ms", "client_id", "secondary_allowed", "secondary_remaining", "replicated"})
	if err != nil {
		fmt.Println("[gossip-delay] error creating result writer:", err)
		return
	}
	defer writer.Close()

	fmt.Println("[gossip-delay] draining the primary node for a fresh client, then checking when the secondary sees it")

	for _, wait := range waits {
		clientID := fmt.Sprintf("gossip-delay-test-%d", wait.Milliseconds())

		primaryIdx := primaryIndexFor(clientID, len(addresses))
		secondaryIdx := (primaryIdx + 1) % len(addresses)
		primaryClient := clients[addresses[primaryIdx]]
		secondaryClient := clients[addresses[secondaryIdx]]

		drainCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		for i := 0; i < capacity; i++ {
			if _, err := primaryClient.CheckQuota(drainCtx, &pb.QuotaRequest{ClientId: clientID}); err != nil {
				fmt.Printf("[gossip-delay] error draining primary: %v\n", err)
			}
		}
		cancel()

		time.Sleep(wait)

		pollCtx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := secondaryClient.CheckQuota(pollCtx, &pb.QuotaRequest{ClientId: clientID})
		cancel2()
		if err != nil {
			fmt.Printf("[gossip-delay] error polling secondary: %v\n", err)
			continue
		}

		// A never-before-touched bucket, if created fresh by this very poll,
		// always yields remaining == capacity-1. Once gossip has merged the
		// primary's drained state, the replica keeps refilling from the
		// inherited timestamp — so !resp.Allowed alone isn't reliable at
		// longer waits (refill can restore enough tokens to look "fresh"
		// again). Any remaining below capacity-1 is only possible if a merge
		// already happened before this poll, so it's a more sensitive signal.
		replicated := resp.RemainingTokens < int32(capacity-1)
		fmt.Printf("[gossip-delay] wait=%s client=%s secondary_allowed=%v secondary_remaining=%d replicated=%v\n",
			wait, clientID, resp.Allowed, resp.RemainingTokens, replicated)

		writer.WriteRow(
			strconv.FormatInt(wait.Milliseconds(), 10),
			clientID,
			strconv.FormatBool(resp.Allowed),
			strconv.Itoa(int(resp.RemainingTokens)),
			strconv.FormatBool(replicated),
		)
	}
}
