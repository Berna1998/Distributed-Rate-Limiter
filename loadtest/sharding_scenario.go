package main

import (
	"fmt"
	"strconv"

	"distributed-rate-limiter/internal/config"
)

// runShardingScenario checks that the hash-based routing spreads many
// distinct clients roughly evenly across aggregator nodes. This is computed
// locally with the same pure hash function used by the edge, rather than
// exercised against a live cluster, since the property under test (fairness
// of the hash) doesn't depend on runtime state.
func runShardingScenario(_ Config) {
	const numClients = 2000
	numNodes := len(config.AggregatorAddresses)

	counts := make([]int, numNodes)
	for i := 0; i < numClients; i++ {
		clientID := fmt.Sprintf("sharding-test-client-%d", i)
		idx := primaryIndexFor(clientID, numNodes)
		counts[idx]++
	}

	writer, err := NewResultWriter("loadtest/results/sharding.csv",
		[]string{"node_index", "client_count", "percentage"})
	if err != nil {
		fmt.Println("[sharding] error creating result writer:", err)
		return
	}
	defer writer.Close()

	fmt.Printf("[sharding] distribution over %d synthetic clients across %d nodes:\n", numClients, numNodes)
	for i, c := range counts {
		pct := float64(c) / float64(numClients) * 100
		fmt.Printf("  aggregator%d: %d clients (%.1f%%)\n", i+1, c, pct)
		writer.WriteRow(strconv.Itoa(i+1), strconv.Itoa(c), fmt.Sprintf("%.2f", pct))
	}
}
