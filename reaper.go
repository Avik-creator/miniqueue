package miniqueue

import (
	"context"
	"log/slog"
	"time"
)

// ReaperConfig configures the Reaper.
type ReaperConfig struct {
	// Interval is how often the reaper scans for expired leases.
	// Default: 5s. Shorter = faster recovery but more DB load.
	Interval time.Duration

	// Logger for operational messages. Default: slog.Default().
	Logger *slog.Logger
}

// Reaper is a background goroutine that recovers jobs whose leases have
// expired. It is the core crash recovery mechanism:
//
// When a worker dies (SIGKILL, OOM, network partition) without calling
// Complete or Fail, its jobs remain in 'running' state with an expired
// lease. The Reaper periodically scans for these orphaned jobs and resets
// them to 'available' so other workers can claim them.
//
// Without a Reaper, a crashed worker's jobs would be stuck in 'running'
// forever. With a Reaper, the worst case recovery time is:
//
//	LeaseDuration + ReaperInterval
//
// For example, with a 30s lease and 5s reaper interval, a crashed
// worker's jobs are recovered within 35 seconds.
type Reaper struct {
	store  *Store
	config ReaperConfig
	log    *slog.Logger
}

// NewReaper creates a Reaper. Call Start() to begin the recovery loop.
func NewReaper(store *Store, config ReaperConfig) *Reaper {
	if config.Interval <= 0 {
		config.Interval = 5 * time.Second
	}
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	return &Reaper{
		store:  store,
		config: config,
		log:    config.Logger.With("component", "reaper"),
	}
}

// Start runs the reaper loop and blocks until ctx is cancelled.
// Typically started as a goroutine: go reaper.Start(ctx).
func (r *Reaper) Start(ctx context.Context) {
	ticker := time.NewTicker(r.config.Interval)
	defer ticker.Stop()

	r.log.Info("reaper starting", "interval", r.config.Interval)

	for {
		select {
		case <-ctx.Done():
			r.log.Info("reaper stopping")
			return
		case <-ticker.C:
			recovered, err := r.store.RecoverExpiredLeases(ctx)
			if err != nil {
				r.log.Error("recovery scan failed", "error", err)
				continue
			}
			if recovered > 0 {
				r.log.Info("recovered expired leases", "count", recovered)
			}
		}
	}
}