package main

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"distributed-rate-limiter/internal/config"
)

// runConcurrencyScenario fires increasing numbers of concurrent requests for
// a single (fresh) client_id and counts how many were allowed. Since a
// client always has one primary aggregator, correctness here does not depend
// on gossip — it depends on the bucket's mutex correctly serializing
// concurrent Allow() calls. The expected result at every concurrency level
// is exactly min(concurrency, capacity) allowed; anything more indicates a
// race condition in the token bucket, regardless of how much load is applied.
func runConcurrencyScenario(cfg Config) {
	levels := []int{5, 20, 50, 200}
	capacity := int64(config.DefaultCapacity)

	writer, err := NewResultWriter("loadtest/results/concurrency.csv",
		[]string{"concurrency", "client_id", "allowed_count", "rejected_count", "excess_over_capacity"})
	if err != nil {
		fmt.Println("[concurrency] error creating result writer:", err)
		return
	}
	defer writer.Close()

	for _, k := range levels {
		clientID := fmt.Sprintf("concurrency-test-%d", k) // fresh bucket per level

		var allowed int64
		var mu sync.Mutex
		var wg sync.WaitGroup

		wg.Add(k)
		for i := 0; i < k; i++ {
			go func() {
				defer wg.Done()
				res := callEdge(cfg.EdgeURL, clientID)
				if res.Allowed {
					mu.Lock()
					allowed++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		rejected := int64(k) - allowed
		excess := allowed - capacity
		if excess < 0 {
			excess = 0
		}

		fmt.Printf("[concurrency] K=%d client=%s allowed=%d rejected=%d excess_over_capacity=%d\n",
			k, clientID, allowed, rejected, excess)

		writer.WriteRow(
			strconv.Itoa(k),
			clientID,
			strconv.FormatInt(allowed, 10),
			strconv.FormatInt(rejected, 10),
			strconv.FormatInt(excess, 10),
		)

		time.Sleep(500 * time.Millisecond)
	}
}
