package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"distributed-rate-limiter/internal/config"
)

// runAlertSensitivityScenario runs two client profiles concurrently for a
// full alerting window (60s): one "honest but bursty" client sending just
// above the sustained refill rate, and one "aggressive" client flooding as
// fast as possible. It then reads analytics' /stats to see how many
// rejections each actually produced, so the current threshold
// (config.AnalyticsAlertThreshold) can be judged against real numbers
// instead of a guess.
func runAlertSensitivityScenario(cfg Config) {
	const duration = 60 * time.Second

	profiles := map[string]time.Duration{
		"alert-test-honest":     2200 * time.Millisecond, // just above the 0.5 token/s refill rate (1 every 2s): rarely rejected
		"alert-test-aggressive": 50 * time.Millisecond,    // far above budget: floods rejections
	}

	writer, err := NewResultWriter("loadtest/results/alert_sensitivity.csv",
		[]string{"client_id", "profile_interval_ms", "rejections_in_window"})
	if err != nil {
		fmt.Println("[alert-sensitivity] error creating result writer:", err)
		return
	}
	defer writer.Close()

	fmt.Printf("[alert-sensitivity] running two client profiles for %s, then reading analytics /stats\n", duration)

	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)

	for clientID, interval := range profiles {
		wg.Add(1)
		go func(clientID string, interval time.Duration) {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for time.Now().Before(deadline) {
				callEdge(cfg.EdgeURL, clientID)
				<-ticker.C
			}
		}(clientID, interval)
	}
	wg.Wait()

	// give the last violation events time to flow through NATS before reading /stats
	time.Sleep(2 * time.Second)

	stats, err := fetchAnalyticsStats(cfg.AnalyticsURL)
	if err != nil {
		fmt.Println("[alert-sensitivity] error reading analytics stats:", err)
		return
	}

	for clientID, interval := range profiles {
		count := stats[clientID]
		fmt.Printf("[alert-sensitivity] %s (every %s): %d rejections in window -> %s\n",
			clientID, interval, count, alertVerdict(count))
		writer.WriteRow(clientID, strconv.FormatInt(interval.Milliseconds(), 10), strconv.Itoa(count))
	}
}

func alertVerdict(count int) string {
	if count >= config.AnalyticsAlertThreshold {
		return "avrebbe fatto scattare l'alert"
	}
	return "non avrebbe fatto scattare l'alert"
}

func fetchAnalyticsStats(url string) (map[string]int, error) {
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var stats map[string]int
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil {
		return nil, err
	}
	return stats, nil
}
