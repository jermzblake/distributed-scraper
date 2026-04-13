package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jermzblake/distributed-scraper/worker"
)

func main() {
	// Command-line flags let you launch worker-1 and worker-2
	// from separate terminal windows, or even on separate machines without recompiling.
	id := flag.String("id", "worker-1", "Unique worker ID (e.g. worker-1)")
	redisAddr := flag.String("redis", "localhost:6379", "Redis server address")
	host := flag.String("host", "books.toscrape.com", "Only crawl URLs from this host (e.g. books.toscrape.com)")
	flag.Parse()

	// Structured logging - each log line includes the worker ID
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// --- Graceful shutdown via OS signals ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := worker.Config{
		ID: *id,
		RedisAddr: *redisAddr,
		PopTimeout: 5 * time.Second,	// Short timeout to check for shutdown signals
		AllowedHost: *host,
	}

	w, err := worker.NewWorker(cfg)
	if err != nil {
		logger.Error("Failed to create worker", "error", err)
		os.Exit(1)
	}

	// Run is blocking - it exits when ctx is cancelled (e.g. on SIGINT/SIGTERM)
	w.Run(ctx)

	logger.Info("Shutdown complete")
}