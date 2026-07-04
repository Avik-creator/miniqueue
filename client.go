package miniqueue

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Client is the high-level API for miniqueue.
// Producers use Client.Enqueue to submit jobs.
// Workers use the Store directly (or a future Worker type in Phase 2).
type Client struct {
	store *Store
}

// NewClient creates a Client connected to Postgres via the given pool.
// The caller must have already run migrations.
func NewClient(pool *pgxpool.Pool) *Client {
	return &Client{store: NewStore(pool)}
}

// Store returns the underlying Store for direct access (e.g., by workers).
func (c *Client) Store() *Store {
	return c.store
}

// Enqueue submits a new job to the named queue.
func (c *Client) Enqueue(ctx context.Context, opts EnqueueOptions) (*Job, error) {
	return c.store.Enqueue(ctx, opts)
}

// RunMigrations executes all SQL migration files in the given directory.
// Migrations are run in lexicographic order (001_..., 002_..., etc.).
// This is a simple, no-dependency migration runner — good enough for Phase 1.
func RunMigrations(ctx context.Context, pool *pgxpool.Pool, migrationsDir string) error {
	entries, err := os.ReadDir(migrationsDir)
	if err != nil {
		return fmt.Errorf("miniqueue: read migrations dir: %w", err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if filepath.Ext(entry.Name()) != ".sql" {
			continue
		}

		path := filepath.Join(migrationsDir, entry.Name())
		sql, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("miniqueue: read migration %s: %w", entry.Name(), err)
		}

		if _, err := pool.Exec(ctx, string(sql)); err != nil {
			return fmt.Errorf("miniqueue: run migration %s: %w", entry.Name(), err)
		}
	}
	return nil
}

// DefaultLeaseDuration is the default lease duration for claimed jobs.
// Workers must complete or renew before this expires.
const DefaultLeaseDuration = 30 * time.Second
