package main

import (
	"testing"
	"time"
)

func TestCircuitBreaker_OpensAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 5*time.Second)

	for i := 0; i < 2; i++ {
		if !cb.Allow() {
			t.Fatalf("expected closed breaker to allow call #%d", i+1)
		}
		cb.RecordFailure()
	}

	if cb.State() != StateClosed {
		t.Fatalf("expected still closed after 2 failures, got %v", cb.State())
	}

	if !cb.Allow() {
		t.Fatal("expected closed breaker to allow call before 3rd failure")
	}
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Fatalf("expected open after 3 consecutive failures, got %v", cb.State())
	}

	if cb.Allow() {
		t.Fatal("expected open breaker to block calls before cooldown elapses")
	}
}

func TestCircuitBreaker_SuccessResetsFailureCount(t *testing.T) {
	cb := NewCircuitBreaker(3, 5*time.Second)

	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordSuccess() // should reset the counter

	cb.Allow()
	cb.RecordFailure()
	cb.Allow()
	cb.RecordFailure()

	if cb.State() != StateClosed {
		t.Fatalf("expected closed (only 2 failures since last success), got %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenAfterCooldown(t *testing.T) {
	fakeNow := time.Now()
	cb := NewCircuitBreaker(1, 5*time.Second)
	cb.now = func() time.Time { return fakeNow }

	cb.Allow()
	cb.RecordFailure() // threshold=1 -> opens immediately

	if cb.State() != StateOpen {
		t.Fatalf("expected open, got %v", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected open breaker to block calls before cooldown")
	}

	fakeNow = fakeNow.Add(5 * time.Second)

	if !cb.Allow() {
		t.Fatal("expected the half-open trial to be allowed once the cooldown has elapsed")
	}
	if cb.State() != StateHalfOpen {
		t.Fatalf("expected half-open, got %v", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected only one trial request to be admitted while half-open")
	}
}

func TestCircuitBreaker_HalfOpenSuccessCloses(t *testing.T) {
	fakeNow := time.Now()
	cb := NewCircuitBreaker(1, 5*time.Second)
	cb.now = func() time.Time { return fakeNow }

	cb.Allow()
	cb.RecordFailure()
	fakeNow = fakeNow.Add(5 * time.Second)
	cb.Allow() // consumes the half-open trial
	cb.RecordSuccess()

	if cb.State() != StateClosed {
		t.Fatalf("expected closed after a successful trial, got %v", cb.State())
	}
	if !cb.Allow() {
		t.Fatal("expected the breaker to allow calls again after closing")
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	fakeNow := time.Now()
	cb := NewCircuitBreaker(1, 5*time.Second)
	cb.now = func() time.Time { return fakeNow }

	cb.Allow()
	cb.RecordFailure()
	fakeNow = fakeNow.Add(5 * time.Second)
	cb.Allow() // consumes the half-open trial
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Fatalf("expected re-opened after a failed trial, got %v", cb.State())
	}
	if cb.Allow() {
		t.Fatal("expected the freshly re-opened breaker to block calls immediately")
	}
}
