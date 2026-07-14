package main

import (
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"
)

// runLoadScenario sends one request from an increasing number of distinct,
// never-seen-before clients at each level, so every request is a client's
// first (always Allowed=true) — this isolates pure system latency from
// rate-limit-decision noise, giving a clean scalability curve.
func runLoadScenario(cfg Config) {
	levels := []int{10, 50, 200, 1000}

	raw, err := NewResultWriter("loadtest/results/load_raw.csv",
		[]string{"level", "client_id", "status_code", "latency_ms"})
	if err != nil {
		fmt.Println("[load] error creating raw result writer:", err)
		return
	}
	defer raw.Close()

	summary, err := NewResultWriter("loadtest/results/load_summary.csv",
		[]string{"level", "p50_ms", "p95_ms", "p99_ms", "error_count"})
	if err != nil {
		fmt.Println("[load] error creating summary result writer:", err)
		return
	}
	defer summary.Close()

	for _, n := range levels {
		warmup(cfg.EdgeURL, n)

		latencies := make([]float64, 0, n)
		var errCount int64
		var mu sync.Mutex
		var wg sync.WaitGroup

		wg.Add(n)
		for i := 0; i < n; i++ {
			clientID := fmt.Sprintf("load-test-client-%d-%d", n, i)
			go func(clientID string) {
				defer wg.Done()
				res := callEdge(cfg.EdgeURL, clientID)

				mu.Lock()
				defer mu.Unlock()
				if res.Err != nil {
					errCount++
				} else {
					latencies = append(latencies, res.LatencyMs)
				}
				raw.WriteRow(strconv.Itoa(n), clientID, strconv.Itoa(res.StatusCode), fmt.Sprintf("%.2f", res.LatencyMs))
			}(clientID)
		}
		wg.Wait()

		sort.Float64s(latencies)
		p50 := percentile(latencies, 50)
		p95 := percentile(latencies, 95)
		p99 := percentile(latencies, 99)

		fmt.Printf("[load] N=%d p50=%.1fms p95=%.1fms p99=%.1fms errors=%d\n", n, p50, p95, p99, errCount)

		summary.WriteRow(
			strconv.Itoa(n),
			fmt.Sprintf("%.2f", p50),
			fmt.Sprintf("%.2f", p95),
			fmt.Sprintf("%.2f", p99),
			strconv.FormatInt(errCount, 10),
		)

		time.Sleep(1 * time.Second)
	}
}

// warmup sends a small burst of throwaway requests before each measured
// level and discards the results. It absorbs one-time costs (first TCP
// connection through the Docker/WSL2 NAT path, Go's HTTP transport lazily
// initializing, etc.) that would otherwise inflate whichever level happens
// to run first, making levels incomparable. It uses client IDs distinct from
// the ones about to be measured so it never touches their token buckets.
func warmup(edgeURL string, level int) {
	const warmupRequests = 20

	var wg sync.WaitGroup
	wg.Add(warmupRequests)
	for i := 0; i < warmupRequests; i++ {
		clientID := fmt.Sprintf("warmup-%d-%d", level, i)
		go func(clientID string) {
			defer wg.Done()
			callEdge(edgeURL, clientID)
		}(clientID)
	}
	wg.Wait()

	time.Sleep(200 * time.Millisecond)
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p / 100 * float64(len(sorted)-1))
	return sorted[idx]
}
