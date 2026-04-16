package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"distributed-scraper/embedder"
	"distributed-scraper/queue"
	"distributed-scraper/worker"
)

func main() {
	// Command-line flags let you launch worker-1 and worker-2
	// from separate terminal windows, or even on separate machines without recompiling.
	id := flag.String("id", "worker-1", "Unique worker ID (e.g. worker-1)")
	redisAddr := flag.String("redis", "localhost:6379", "Redis server address")
	qdrantAddr := flag.String("qdrant", "localhost:6334", "Qdrant gRPC address")
	host := flag.String("host", "books.toscrape.com", "Only crawl URLs from this host (e.g. books.toscrape.com)")
	ratePerSec := flag.Float64("rate", 2.0, "requests per second per domain")
	rateBurst := flag.Int("burst", 5, "max burst requests")
	ollamaAddr := flag.String("ollama", "http://localhost:11434", "ollama address (e.g. http://localhost:11434)")
	model := flag.String("model", embedder.ModelQwen3_0_6B, "ollama embedding model")
	dims := flag.Uint64("dims", embedder.DefaultDimension, "embedding dimension")
	export := flag.Bool("export", false, "Dump scraped results to JSON and exit")
	out := flag.String("out", "results.json", "Output file for -export (use - for stdout)")
	reset := flag.Bool("reset", false, "After -export, delete scraper:results and scraper:seen from Redis")
	flag.Parse()

	// In export mode, send logs to stderr so JSON written to stdout stays clean.
	logDest := os.Stdout
	if *export && *out == "-" {
		logDest = os.Stderr
	}

	// Structured logging - each log line includes the worker ID
	logger := slog.New(slog.NewTextHandler(logDest, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	// --- Graceful shutdown via OS signals ---
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// --- Export mode: dump results from Redis and exit ---
	if *reset && !*export {
		logger.Warn("-reset has no effect without -export")
	}
	if *export {
		q := queue.New(*redisAddr)
		defer q.Close()

		if err := q.Ping(ctx); err != nil {
			logger.Error("Failed to connect to Redis", "error", err)
			os.Exit(1)
		}

		count, err := q.ResultsCount(ctx)
		if err != nil {
			logger.Error("Failed to check results count", "error", err)
			os.Exit(1)
		}
		if count == 0 {
			logger.Info("No results to export — run a crawl first")
			return
		}

		var w *os.File
		if *out == "-" {
			w = os.Stdout
		} else {
			var err error
			w, err = os.Create(*out)
			if err != nil {
				logger.Error("Failed to create output file", "file", *out, "error", err)
				os.Exit(1)
			}
			defer w.Close()
		}

		if err := q.ExportResults(ctx, w); err != nil {
			logger.Error("Export failed", "error", err)
			os.Exit(1)
		}

		if *out != "-" {
			logger.Info("Results exported", "file", *out)
		}

		if *reset {
			if err := q.Reset(ctx); err != nil {
				logger.Error("Reset failed", "error", err)
				os.Exit(1)
			}
			logger.Info("Redis keys cleared", "keys", []string{"scraper:results", "scraper:seen"})
		}

		return
	}

	if *reset {
		logger.Warn("-reset has no effect without -export")
	}

	cfg := worker.Config{
		ID:             *id,
		RedisAddr:      *redisAddr,
		PopTimeout:     5 * time.Second, // Short timeout to check for shutdown signals
		QdrantAddr:     *qdrantAddr,
		AllowedHost:    *host,
		RatePerSecond:  *ratePerSec,
		RateBurst:      *rateBurst,
		ChunkSize:      400,
		ChunkOverlap:   50,
		OllamaAddr:     *ollamaAddr,
		EmbedModel:     *model,
		EmbedDimension: *dims,
	}

	wk, err := worker.NewWorker(cfg)
	if err != nil {
		logger.Error("Failed to create worker", "error", err)
		os.Exit(1)
	}

	// Run is blocking - it exits when ctx is cancelled (e.g. on SIGINT/SIGTERM)
	wk.Run(ctx)

	logger.Info("Shutdown complete")
}
