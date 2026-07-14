package main

import (
	"fmt"
	"strconv"
	"time"

	"distributed-rate-limiter/internal/config"
)

// runFailoverScenario sends sustained load (1 req/s) for a single client for
// a full minute, and prints exactly when and which aggregator container to
// stop/restart so you can reproduce the manual chaos test while the script
// records latency and allow/degraded outcomes throughout. The kill/restart
// itself stays manual on purpose — this tool only measures.
func runFailoverScenario(cfg Config) {
	const (
		clientID  = "failover-test-client"
		totalDur  = 60 * time.Second
		tickEvery = 1 * time.Second
		killAt    = 15 * time.Second
		restoreAt = 40 * time.Second
	)

	primaryIdx := primaryIndexFor(clientID, len(config.AggregatorAddresses))
	primaryLabel := fmt.Sprintf("aggregator%d", primaryIdx+1)

	writer, err := NewResultWriter("loadtest/results/failover.csv",
		[]string{"elapsed_s", "status_code", "allowed", "degraded", "latency_ms"})
	if err != nil {
		fmt.Println("[failover] error creating result writer:", err)
		return
	}
	defer writer.Close()

	fmt.Printf("[failover] running for %s against client %q (primary: %s), 1 request/sec\n", totalDur, clientID, primaryLabel)

	start := time.Now()
	ticker := time.NewTicker(tickEvery)
	defer ticker.Stop()

	killPrinted, restorePrinted := false, false

	for now := range ticker.C {
		elapsed := now.Sub(start)
		if elapsed > totalDur {
			break
		}

		if !killPrinted && elapsed >= killAt {
			fmt.Printf(">>> ORA: esegui `docker compose stop %s` (nodo primario di %s) <<<\n", primaryLabel, clientID)
			killPrinted = true
		}
		if !restorePrinted && elapsed >= restoreAt {
			fmt.Printf(">>> ORA: esegui `docker compose start %s` <<<\n", primaryLabel)
			restorePrinted = true
		}

		res := callEdge(cfg.EdgeURL, clientID)
		fmt.Printf("[failover] t=%.0fs status=%d allowed=%v degraded=%v latency=%.1fms\n",
			elapsed.Seconds(), res.StatusCode, res.Allowed, res.Degraded, res.LatencyMs)

		writer.WriteRow(
			fmt.Sprintf("%.0f", elapsed.Seconds()),
			strconv.Itoa(res.StatusCode),
			strconv.FormatBool(res.Allowed),
			strconv.FormatBool(res.Degraded),
			fmt.Sprintf("%.2f", res.LatencyMs),
		)
	}

	fmt.Println("[failover] done, see loadtest/results/failover.csv")
}
