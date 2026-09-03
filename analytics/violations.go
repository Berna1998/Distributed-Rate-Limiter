package main

import (
	"log"
	"sync"
	"time"
)

// ViolationStore conta le richieste rifiutate per ogni client all'interno
// di una finestra fissa, azzerando tutti i contatori a ogni tick
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

// registra una richiesta respinta per clientID e genera un avviso la
// prima volta che il client supera la soglia all'interno della finestra corrente.
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
