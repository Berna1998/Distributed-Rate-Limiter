package main

import (
	"sync"
	"time"
)

type Bucket struct {
	Tokens      float64
	Capacity    float64
	RefillRate  float64
	LastRefill  time.Time
	LastUpdated time.Time
	Dirty       bool
	mu          sync.Mutex
}

func NewBucket(capacity, refillRate float64) *Bucket {
	now := time.Now()
	return &Bucket{
		Tokens:      capacity,
		Capacity:    capacity,
		RefillRate:  refillRate,
		LastRefill:  now,
		LastUpdated: now,
	}
}

func (b *Bucket) refill() {

	now := time.Now()

	oldTokens := b.Tokens
	elapsed := now.Sub(b.LastRefill).Seconds()
	b.Tokens += elapsed * b.RefillRate

	if b.Tokens > b.Capacity {
		b.Tokens = b.Capacity
	}

	if b.Tokens != oldTokens {
		b.LastUpdated = now
	}

	b.LastRefill = now
}

func (b *Bucket) Allow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()

	// refill
	elapsed := now.Sub(b.LastRefill).Seconds()
	if elapsed > 0 {
		b.Tokens += elapsed * b.RefillRate
		if b.Tokens > b.Capacity {
			b.Tokens = b.Capacity
		}
		b.LastRefill = now
	}

	// check tokens
	if b.Tokens < 1 {
		b.LastUpdated = now
		b.Dirty = true
		return false
	}

	b.Tokens--
	b.LastUpdated = now
	
	b.Dirty = true

	return true
}
