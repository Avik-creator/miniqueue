package miniqueue

import (
	"math"
	"math/rand"
	"time"
)

// DefaultBackoff computes the retry delay for a given attempt number using
// exponential backoff with full jitter.
//
// The formula is: delay = random(0, min(cap, base * 2^attempt))
//
// This is the "Full Jitter" strategy from the AWS Architecture Blog post
// "Exponential Backoff And Jitter" (2015). It prevents thundering herds
// when multiple jobs fail simultaneously and retry at the same time.
//
// Example with base=1s, cap=30m:
//
//	attempt 1: 0-2s
//	attempt 2: 0-4s
//	attempt 3: 0-8s
//	attempt 4: 0-16s
//	attempt 5: 0-30m (capped)
//
// The jitter ensures that even if 1000 jobs fail at the same time, their
// retries are spread across the backoff window instead of all hitting the
// database at once.
func DefaultBackoff(attempt int) time.Duration {
	const (
		base = 1 * time.Second
		cap  = 30 * time.Minute
	)

	// Compute base * 2^attempt
	pow := math.Pow(2, float64(attempt))
	expBackoff := time.Duration(float64(base) * pow)

	// Cap the backoff
	if expBackoff > cap {
		expBackoff = cap
	}

	// Add full jitter: random value between 0 and expBackoff
	jitter := time.Duration(rand.Int63n(int64(expBackoff) + 1))
	return jitter
}

// BackoffFunc is the signature for backoff computation functions.
// It takes the attempt number (1-indexed) and returns the delay before retry.
type BackoffFunc func(attempt int) time.Duration
