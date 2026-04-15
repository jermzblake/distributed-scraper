package worker

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"distributed-scraper/chunker"
	"distributed-scraper/embedder"
	"distributed-scraper/queue"
	"distributed-scraper/scraper"
	"distributed-scraper/vectorstore"
)

// Config controls worker behavior.
type Config struct {
	ID             string        // Human-readable name, e.g. "worker-1"
	RedisAddr      string        // Redis connection string, e.g. "localhost:6379"
	PopTimeout     time.Duration // How long to block on BRPOP before looping
	MaxDepth       int           // Maximum crawl depth (Don't follow links beyond this depth (0 = seed only))
	AllowedHost    string        // Only crawl URLs from this host
	QdrantAddr     string        // e.g. "localhost:6334" (gRPC port)
	RatePerSecond  float64       // requests per second per domain
	RateBurst      int           // max burst
	ChunkSize      int
	ChunkOverlap   int
	OllamaAddr     string // NEW: replaces OpenAIKey
	EmbedModel     string // NEW: e.g. "qwen3-embedding:0.6b"
	EmbedDimension uint64 // NEW: must match model output
}

type Worker struct {
	cfg      Config
	queue    *queue.Queue
	embedder *embedder.Embedder
	store    *vectorstore.Store
	limiter  *queue.RateLimiter
	logger   *slog.Logger
}

func NewWorker(cfg Config) (*Worker, error) {
	q := queue.New(cfg.RedisAddr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := q.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis at %s: %w", cfg.RedisAddr, err)
	}

	emb := embedder.New(cfg.OllamaAddr, cfg.EmbedModel, cfg.EmbedDimension)

	store, err := vectorstore.New(cfg.QdrantAddr, false)
	if err != nil {
		return nil, fmt.Errorf("qdrant: %w", err)
	}

	// Ensure the collection exists with the right vector dimension
	if err := store.EnsureCollection(ctx, emb.Dimension()); err != nil {
		return nil, fmt.Errorf("ensure collection: %w", err)
	}

	// Share the redis client for rate limiting (reuse the connection pool)
	limiter := queue.NewRateLimiter(q.Client(), cfg.RatePerSecond, cfg.RateBurst)

	logger := slog.Default().With("worker", cfg.ID)
	return &Worker{
		cfg:      cfg,
		queue:    q,
		embedder: emb,
		store:    store,
		limiter:  limiter,
		logger:   logger,
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

		if err := w.processUrl(ctx, url); err != nil {
			w.logger.Warn("process error", "url", url, "err", err)
		}
	}
}

func (w *Worker) processUrl(ctx context.Context, rawURL string) error {
	// --- Deduplication (atomic check-and-mark) ---
	isNew, err := w.queue.MarkSeen(ctx, rawURL)
	if err != nil {
		return fmt.Errorf("Failed to mark URL as seen: %w", err)
	}
	if !isNew {
		w.logger.Debug("URL already seen, skipping", "url", rawURL)
		return nil
	}

	// Rate limit - wait for a token for this domain
	domain := hostOf(rawURL)
	if err := w.limiter.Wait(ctx, domain); err != nil {
		return fmt.Errorf("Rate limit wait failed for domain %s: %w", domain, err)
	}

	w.logger.Info("scraping", "url", rawURL)

	// --- Fetch + Parse ---
	page, err := scraper.Fetch(ctx, rawURL)
	if err != nil {
		return fmt.Errorf("Failed to fetch URL %s: %w", rawURL, err)
	}
	if page.Content == "" {
		w.logger.Debug("Empty content, skipping", "url", rawURL)
		return nil
	}

	// Chunk the content
	// Prepend title + headings to the content so each chunk has document context.
	// This significantly improves retrieval for questions about the page's topic.
	// Build two representations of the same content:
		// 1. enrichedText  → fed to the embedder (never stored)
		// 2. rawChunks     → stored in Qdrant payload (what you display)
	rawChunks := chunker.Split(page.Content, w.cfg.ChunkSize, w.cfg.ChunkOverlap)
	if len(rawChunks) == 0 {
			return nil
	}

	// Build enriched versions for embedding only
	enrichedTexts := make([]string, len(rawChunks))
	for i, c := range rawChunks {
			enrichedTexts[i] = buildEnrichedChunk(page, c.Text)
	}

	// Embed the enriched text
	vectors, err := w.embedder.EmbedBatch(ctx, enrichedTexts)
	if err != nil {
			return fmt.Errorf("embed: %w", err)
	}

	// Build Qdrant points
	var points []vectorstore.Point
	for i, chunk := range rawChunks {
		if vectors[i] == nil {
			continue // Skip chunks that failed to embed
		}
		points = append(points, vectorstore.Point{
			// Deterministic ID: same chunk always overwrites itself in Qdrant
			ChunkID:    fmt.Sprintf("%s#chunk-%d", rawURL, chunk.Index),
			Vector:     vectors[i],
			URL:        rawURL,
			Title:      page.Title,
			ChunkIndex: chunk.Index,
			ChunkText:  chunk.Text,
			WorkerID:   w.cfg.ID,
		})
	}

	// Upsert to Qdrant
	if err := w.store.Upsert(ctx, points); err != nil {
		w.logger.Error("Failed to upsert to vector store", "url", rawURL, "error", err)
		return err
	}

	w.logger.Info("Indexed page", "url", rawURL, "chunks", len(points))

	// --- Save Result ---
	doc := queue.ScrapedDoc{
		URL:       page.URL,
		Title:     page.Title,
		Content:   page.Content,
		Links:     page.Links,
		ScrapedAt: time.Now().UTC(),
		WorkerID:  w.cfg.ID,
	}
	if err := w.queue.SaveResult(ctx, doc); err != nil {
		w.logger.Error("Failed to save scraped document", "url", rawURL, "error", err)
		// Don't return - still enqueue links even if saving failed
	}

	// --- Enqueue Discovered Links ---
	enqueued := 0
	for _, link := range page.Links {
		if w.cfg.AllowedHost != "" && hostOf(link) != w.cfg.AllowedHost {
			continue
		}
		if err := w.queue.Push(ctx, link); err == nil {
			enqueued++
		}
	}

	w.logger.Info("Finished processing URL",
		"url", rawURL,
		"title", page.Title,
		"links_found", len(page.Links),
		"links_enqueued", enqueued,
	)

	return nil
}

// buildEnrichedChunk constructs the text fed to the embedder.
// This is NEVER stored — only the raw chunkText goes into Qdrant.
func buildEnrichedChunk(page *scraper.Page, chunkText string) string {
    var sb strings.Builder
    if page.Title != "" {
        sb.WriteString("Title: ")
        sb.WriteString(page.Title)
        sb.WriteString("\n")
    }
    if page.Description != "" {
        sb.WriteString(page.Description)
        sb.WriteString("\n")
    }
    sb.WriteString(chunkText)
    return sb.String()
}

func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	return u.Hostname()
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
