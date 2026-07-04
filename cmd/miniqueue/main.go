package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	miniqueue "avikmukherjee.com/miniqueue"
)

func main() {
	// --- Configuration ---
	// In production, these come from env vars or a config file.
	// For the demo, we use sensible defaults that work with the
	// Docker Postgres from your dev setup.
	databaseURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/miniqueue_dev?sslmode=disable")
	queueName := getEnv("QUEUE", "default")
	workerID := getEnv("WORKER_ID", fmt.Sprintf("worker-%d", os.Getpid()))

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	// --- Phase 1: Connect to Postgres ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Error("failed to connect to database", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Error("failed to ping database", "error", err)
		os.Exit(1)
	}
	log.Info("connected to database")

	// --- Phase 2: Run migrations ---
	if err := miniqueue.RunMigrations(ctx, pool, "migrations"); err != nil {
		log.Error("failed to run migrations", "error", err)
		os.Exit(1)
	}
	log.Info("migrations applied")

	// --- Phase 3: Build the components ---
	store := miniqueue.NewStore(pool)
	client := miniqueue.NewClient(pool)

	// The handler is YOUR business logic. This is where you'd send emails,
	// process payments, resize images, etc. For the demo, we just log.
	handler := miniqueue.HandlerFunc(func(ctx context.Context, job *miniqueue.Job) error {
		log.Info("📨 processing job",
			"job_id", job.ID,
			"queue", job.Queue,
			"attempt", job.Attempt,
			"payload", string(job.Payload),
		)

		// Simulate real work.
		time.Sleep(500 * time.Millisecond)

		// To simulate a failure, return an error. Uncomment to test:
		// return fmt.Errorf("simulated failure for job %d", job.ID)

		log.Info("✅ job done", "job_id", job.ID)
		return nil
	})

	worker := miniqueue.NewWorker(store, handler, miniqueue.WorkerConfig{
		Queue:             queueName,
		WorkerID:          workerID,
		PollInterval:      500 * time.Millisecond,
		LeaseDuration:     30 * time.Second,
		HeartbeatInterval: 10 * time.Second,
		ShutdownTimeout:   15 * time.Second,
	})

	reaper := miniqueue.NewReaper(store, miniqueue.ReaperConfig{
		Interval: 5 * time.Second,
	})

	// --- Phase 4: Seed demo jobs ---
	go seedDemoJobs(ctx, client, queueName, log)

	// --- Phase 5: Start the runtime ---
	//
	// The reaper runs in a goroutine — it's a background maintenance task.
	// The worker's Start() blocks — it IS the main loop.
	go reaper.Start(ctx)

	// --- Phase 6: Graceful shutdown on SIGINT/SIGTERM ---
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Info("received signal, shutting down", "signal", sig)
		cancel()
	}()

	log.Info("worker running — press Ctrl+C to stop",
		"queue", queueName,
		"worker_id", workerID,
	)

	if err := worker.Start(ctx); err != nil {
		log.Error("worker exited with error", "error", err)
		os.Exit(1)
	}

	log.Info("shutdown complete")
}

// seedDemoJobs enqueues a few jobs so you see the worker do something
// immediately after starting. Runs once, then stops.
func seedDemoJobs(ctx context.Context, client *miniqueue.Client, queue string, log *slog.Logger) {
	// Small delay so the worker is ready before we enqueue.
	time.Sleep(1 * time.Second)

	jobs := []struct {
		name     string
		priority int16
	}{
		{"send_welcome_email", 1},
		{"generate_thumbnail", 5},
		{"process_payment", 10}, // Highest priority — will be claimed first.
		{"send_notification", 1},
		{"sync_inventory", 3},
	}

	for _, j := range jobs {
		payload, _ := json.Marshal(map[string]string{"task": j.name})
		job, err := client.Enqueue(ctx, miniqueue.EnqueueOptions{
			Queue:    queue,
			Payload:  payload,
			Priority: j.priority,
		})
		if err != nil {
			log.Error("failed to enqueue demo job", "error", err, "task", j.name)
			continue
		}
		log.Info("📝 enqueued demo job",
			"job_id", job.ID,
			"task", j.name,
			"priority", j.priority,
		)
	}

	log.Info("demo jobs seeded — watch the worker process them in priority order")
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}