package main

import (
	"sync"
	"time"
)

type breakerState int

const (
	StateClosed breakerState = iota
	StateOpen
	StateHalfOpen
)

func (s breakerState) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateOpen:
		return "open"
	case StateHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

type CircuitBreaker struct {
	mu sync.Mutex

	failureThreshold int
	cooldown         time.Duration
	now              func() time.Time

	state       breakerState
	failures    int
	openedAt    time.Time
	halfOpenTry bool
}

func NewCircuitBreaker(failureThreshold int, cooldown time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: failureThreshold,
		cooldown:         cooldown,
		now:              time.Now,
		state:            StateClosed,
	}
}

// verifica se è possibile effettuare una chiamata in questo momento.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case StateClosed:
		return true
	case StateOpen:
		if cb.now().Sub(cb.openedAt) < cb.cooldown {
			return false
		}
		cb.state = StateHalfOpen
		cb.halfOpenTry = false
		return cb.tryHalfOpenLocked()
	case StateHalfOpen:
		return cb.tryHalfOpenLocked()
	default:
		return false
	}
}

func (cb *CircuitBreaker) tryHalfOpenLocked() bool {
	if cb.halfOpenTry {
		return false
	}
	cb.halfOpenTry = true
	return true
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.failures = 0
	cb.state = StateClosed
	cb.halfOpenTry = false
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == StateHalfOpen {
		cb.openLocked()
		return
	}

	cb.failures++
	if cb.failures >= cb.failureThreshold {
		cb.openLocked()
	}
}

func (cb *CircuitBreaker) openLocked() {
	cb.state = StateOpen
	cb.openedAt = cb.now()
	cb.failures = 0
	cb.halfOpenTry = false
}

func (cb *CircuitBreaker) State() breakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}
