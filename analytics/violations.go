package main

import (
	"log"
	"sync"
	"time"
)

// ViolationStore counts rejected requests per client within a fixed window,
// resetting all counters on a ticker instead of maintaining a true sliding
// window per client. This trades some precision (a burst spanning a reset
// boundary can be undercounted) for a much simpler, race-free implementation
// that is enough to demonstrate the alerting requirement.
type ViolationStore struct {
	mu        sync.Mutex
	counts    map[string]int
	alerted   map[string]bool
	threshold int
}

func NewViolationStore(threshold int, window time.Duration) *ViolationStore {
	s := &ViolationStore{
		counts:    make(map[string]int),
		alerted:   make(map[string]bool),
		threshold: threshold,
	}

	go s.resetLoop(window)

	return s
}

func (s *ViolationStore) resetLoop(window time.Duration) {
	ticker := time.NewTicker(window)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		s.counts = make(map[string]int)
		s.alerted = make(map[string]bool)
		s.mu.Unlock()
	}
}

// Record registers a rejected request for clientID and fires an alert the
// first time the client crosses the threshold within the current window.
func (s *ViolationStore) Record(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.counts[clientID]++

	if s.counts[clientID] >= s.threshold && !s.alerted[clientID] {
		s.alerted[clientID] = true
		log.Printf("ALERT: client %s exceeded the critical rejection threshold (%d rejections in the current window)",
			clientID, s.counts[clientID])
	}
}

func (s *ViolationStore) Snapshot() map[string]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make(map[string]int, len(s.counts))
	for k, v := range s.counts {
		out[k] = v
	}
	return out
}
