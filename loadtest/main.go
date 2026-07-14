package main

import (
	"flag"
	"fmt"
	"os"
)

// Config carries the target URLs every scenario needs; scenarios that don't
// need a particular field simply ignore it.
type Config struct {
	EdgeURL      string
	AnalyticsURL string
}

func main() {
	scenario := flag.String("scenario", "", "which scenario to run: concurrency, load, failover, sharding, gossip-delay, alert-sensitivity")
	edgeURL := flag.String("edge", "http://localhost:8081/api", "edge base URL")
	analyticsURL := flag.String("analytics", "http://localhost:8082/stats", "analytics stats URL")
	flag.Parse()

	cfg := Config{EdgeURL: *edgeURL, AnalyticsURL: *analyticsURL}

	scenarios := map[string]func(Config){
		"concurrency":       runConcurrencyScenario,
		"load":              runLoadScenario,
		"failover":          runFailoverScenario,
		"sharding":          runShardingScenario,
		"gossip-delay":      runGossipDelayScenario,
		"alert-sensitivity": runAlertSensitivityScenario,
	}

	run, ok := scenarios[*scenario]
	if !ok {
		fmt.Fprintf(os.Stderr, "unknown or missing -scenario %q. Available: concurrency, load, failover, sharding, gossip-delay, alert-sensitivity\n", *scenario)
		os.Exit(1)
	}

	run(cfg)
}
