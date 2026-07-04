package miniqueue

import (
	"testing"
	"time"
)

func TestDefaultBackoff(t *testing.T) {
	tests := []struct {
		name     string
		attempt  int
		maxDelay time.Duration
	}{
		{"attempt 1", 1, 2 * time.Second},
		{"attempt 2", 2, 4 * time.Second},
		{"attempt 3", 3, 8 * time.Second},
		{"attempt 4", 4, 16 * time.Second},
		{"attempt 5", 5, 32 * time.Second},
		{"attempt 10", 10, 30 * time.Minute}, // capped
		{"attempt 20", 20, 30 * time.Minute}, // capped
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			delay := DefaultBackoff(tt.attempt)
			if delay < 0 {
				t.Errorf("delay should not be negative: %v", delay)
			}
			if delay > tt.maxDelay {
				t.Errorf("delay %v exceeds max %v for attempt %d", delay, tt.maxDelay, tt.attempt)
			}
		})
	}
}

func TestDefaultBackoff_Jitter(t *testing.T) {
	// Call backoff multiple times and verify we get different values
	// (jitter is working)
	delays := make(map[time.Duration]bool)
	for i := 0; i < 100; i++ {
		delay := DefaultBackoff(3)
		delays[delay] = true
	}

	// With jitter, we should get many different values
	if len(delays) < 10 {
		t.Errorf("expected at least 10 different delay values from 100 calls, got %d (jitter may not be working)", len(delays))
	}
}
