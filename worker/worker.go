package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/jermzblake/distributed-scraper/queue"
	"github.com/jermzblake/distributed-scraper/scraper"
)

// Config controls worker behavior.
type Config struct {
	ID 					string				// Human-readable name, e.g. "worker-1"
	RedisAddr 	string				// Redis connection string, e.g. "localhost:6379"
	PopTimeout 	time.Duration	// How long to block on BRPOP before looping
	MaxDepth 		int						// Maximum crawl depth (Don't follow links beyond this depth (0 = seed only))
	AllowedHost string				// Only crawl URLs from this host
}

type Worker struct {
	cfg Config
	queue *queue.Queue
	logger *slog.Logger
}

func NewWorker(cfg Config) (*Worker, error) {
	q := queue.New(cfg.RedisAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := q.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s: %w", cfg.RedisAddr, err)
	}

	logger := slog.Default().With("worker", cfg.ID)
	return &Worker{
		cfg: cfg,
		queue: q,
		logger: logger,
	}, nil
}

// Run starts the worker loop. It will run until the context is canceled.
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("Worker started", "redis", w.cfg.RedisAddr)
	defer w.logger.Info("Worker stopped")
	defer w.queue.Close()

	for {
		select {
			case <-ctx.Done():
				return
			default:
		}

		url, err := w.queue.Pop(ctx, w.cfg.PopTimeout)
		if err != nil {
			// Context cancelled - clean shutdown
			// Context cancellation will cause BRPOP to return an error, so check for that first
			if ctx.Err() != nil {
				return
			}
			w.logger.Error("Failed to pop from queue", "error", err)
			time.Sleep(1 * time.Second) // Backoff on error
			continue
		}

		if url == "" {
			// Timeout - queue is empty, loop and try again
			w.logger.Debug("queue is empty, waiting...")
			continue
		}

		w.processUrl(ctx, url)
	}
}

func (w *Worker) processUrl(ctx context.Context, rawURL string) {
	// --- Deduplication (atomic check-and-mark) ---
	isNew, err := w.queue.MarkSeen(ctx, rawURL)
	if err != nil {
		w.logger.Error("Failed to mark URL as seen", "url", rawURL, "error", err)
		return
	}
	if !isNew {
		w.logger.Debug("URL already seen, skipping", "url", rawURL)
		return
	}

	w.logger.Info("scraping", "url", rawURL)

	// --- Fetch + Parse ---
	page, err := scraper.Fetch(ctx, rawURL)
	if err != nil {
		w.logger.Warn("Failed to fetch URL", "url", rawURL, "error", err)
		return
	}

	// --- Save Result ---
	doc := queue.ScrapedDoc{
		URL: page.URL,
		Title: page.Title,
		Content: page.Content,
		Links: page.Links,
		ScrapedAt: time.Now().UTC(),
		WorkerID: w.cfg.ID,
	}
	if err := w.queue.SaveResult(ctx, doc); err != nil {
		w.logger.Error("Failed to save scraped document", "url", rawURL, "error", err)
		// Don't return - still enqueue links even if saving failed
	}

	// --- Enqueue Discovered Links ---
	enqueued := 0
	for _, link := range page.Links {
		if !w.shouldFollow(link) {
			continue
		}
		if err := w.queue.Push(ctx, link); err != nil {
			w.logger.Warn("Failed to enqueue link", "url", link, "error", err)
		} else {
			enqueued++
		}
	}

	w.logger.Info("Finished processing URL", 
		"url", rawURL, 
		"title", page.Title,
		"links_found", len(page.Links),
		"links_enqueued", enqueued,
	)
}

// shouldFollow returns true if the worker should enqueue this link
func (w *Worker) shouldFollow(link string) bool {
	if w.cfg.AllowedHost == "" {
		return true
	}
	// Simple check: only follow links that start with the allowed host - in production you'd use url.Parse
	return ContainsHost(link, w.cfg.AllowedHost)
}

func ContainsHost(rawURL, host string) bool {
	if rawURL == "" || host == "" {
		return false
	}

	parsed, err := url.Parse(rawURL)
	if err != nil {
		return false
	}

	return strings.EqualFold(parsed.Hostname(), host)
}