package main

import (
	"distributed-rate-limiter/internal/config"
	"log"
	"sync"
	"time"
)

type BucketManager struct {
	mu      sync.RWMutex
	buckets map[string]*Bucket
	nodeID  string

	lastGossip time.Time
}

type MergeStats struct {
	Created int
	Updated int
	Ignored int
}

func NewBucketManager(nodeID string) *BucketManager {
	return &BucketManager{
		buckets:    make(map[string]*Bucket),
		nodeID:     nodeID,
		lastGossip: time.Time{},
	}
}

// GetBucket used to take a full write lock for every call, serializing all
// requests through one mutex per aggregator regardless of client — under the
// loadtest's "load" scenario this showed up directly as p50 latency growing
// from ~27ms at 10 concurrent clients to ~1.2s at 1000. Existing buckets are
// now looked up under a read lock so unrelated clients' requests can proceed
// in parallel; only the (rare) first-touch path that creates a new bucket
// takes the write lock, with a re-check in case another goroutine created it
// first while we were waiting for that lock.
func (m *BucketManager) GetBucket(clientID string) *Bucket {

	m.mu.RLock()
	bucket, exists := m.buckets[clientID]
	m.mu.RUnlock()

	if exists {
		return bucket
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	bucket, exists = m.buckets[clientID]
	if !exists {
		log.Printf("Creating bucket for %s", clientID)
		bucket = NewBucket(config.DefaultCapacity, config.DefaultRefillRate)
		m.buckets[clientID] = bucket
	}

	return bucket
}

func (m *BucketManager) Snapshot() []BucketState {

	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make([]BucketState, 0, len(m.buckets))

	for clientID, bucket := range m.buckets {

		bucket.mu.Lock()

		state := BucketState{
			ClientID:    clientID,
			Tokens:      bucket.Tokens,
			LastRefill:  bucket.LastRefill,
			LastUpdated: bucket.LastUpdated,
		}

		bucket.mu.Unlock()

		states = append(states, state)
	}

	return states
}

func (m *BucketManager) SnapshotModified() []BucketState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make([]BucketState, 0)

	for clientID, bucket := range m.buckets {

		bucket.mu.Lock()

		if bucket.Dirty {

			states = append(states, BucketState{
				ClientID:    clientID,
				Tokens:      bucket.Tokens,
				LastRefill:  bucket.LastRefill,
				LastUpdated: bucket.LastUpdated,
			})
		}

		bucket.mu.Unlock()
	}

	return states
}

func (m *BucketManager) Merge(states []BucketState) MergeStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := MergeStats{}

	for _, remote := range states {
		local, exists := m.buckets[remote.ClientID]
		// CASE 1: bucket non esiste localmente
		if !exists {
			log.Printf("[%s] Merge: creating bucket %s", m.nodeID, remote.ClientID)
			b := bucketFromState(remote)
			b.Dirty = false // importante: gossip NON è dirty
			m.buckets[remote.ClientID] = b
			stats.Created++
			continue
		}
		// CASE 2: last-write-wins
		if remote.LastUpdated.After(local.LastUpdated) {
			log.Printf("[%s] Merge: updating bucket %s", m.nodeID, remote.ClientID)
			b := bucketFromState(remote)
			b.Dirty = false // gossip non deve ri-triggerare snapshot
			m.buckets[remote.ClientID] = b
			stats.Updated++
		} else {
			stats.Ignored++
		}
	}

	return stats
}

func bucketFromState(state BucketState) *Bucket {
	return &Bucket{
		Tokens:      state.Tokens,
		Capacity:    config.DefaultCapacity,
		RefillRate:  config.DefaultRefillRate,
		LastRefill:  state.LastRefill,
		LastUpdated: state.LastUpdated,
	}
}
