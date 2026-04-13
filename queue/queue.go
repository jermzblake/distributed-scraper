package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	QueueKey		= "scraper:queue"		// List - pending URLs
	SeenKey			= "scraper:seen"		// Set - visited URLs (dedup)
	ResultsKey	= "scraper:results"	// List - completed documents
)

// Queue wraps a Redis client with scraper-specific operations.
type Queue struct {
	client *redis.Client
}

// ScrapedDoc is what a worker produces after visiting a URL.
type ScrapedDoc struct {
	URL     	string   					`json:"url"`
	Title   	string   					`json:"title"`
	Content 	string   					`json:"content"`
	Links   	[]string 					`json:"links"`
	ScrapedAt time.Time 				`json:"scraped_at"`
	WorkerID 	string 						`json:"worker_id"`
	Metadata 	map[string]string `json:"metadata,omitempty"`
}

func New(addr string) *Queue {
	client := redis.NewClient(&redis.Options{
		Addr: addr,
		DialTimeout: 5 * time.Second,
		ReadTimeout: 5 * time.Second,		// Must be > BRPOP timeout in worker
		WriteTimeout: 5 * time.Second,
	})
	return &Queue{client: client}
}

// Ping verifies the Redis connection. Call this at startup.
func (q *Queue) Ping(ctx context.Context) error {
	return q.client.Ping(ctx).Err()
}

// Push adds a URL to the work queue (left push = head of list).
func (q *Queue) Push(ctx context.Context, url string) error {
	return q.client.LPush(ctx, QueueKey, url).Err()
}

// Pop blocks until a URL is available, then returns it (right pop = tail of list).
// timeout=0 blocks forever; use a short timeout (e.g. 5s) so
// the worker can react to shutdown signals.
func (q *Queue) Pop(ctx context.Context, timeout time.Duration) (string, error) {
	// BRPop returns a slice of [queueName, url] if successful, or nil if timeout occurs.
	result, err := q.client.BRPop(ctx, timeout, QueueKey).Result()
	if err == redis.Nil {
		// Timeout - no item available, not an error
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("failed to pop from queue: %w", err)
	}
	return result[1], nil // result[0] is the queue name, result[1] is the URL
}

// MarkSeen adds a URL to the "seen" set. Returns true if it was newly added.
func (q *Queue) MarkSeen(ctx context.Context, url string) (bool, error) {
	n, err := q.client.SAdd(ctx, SeenKey, url).Result()
	if err != nil {
		return false, fmt.Errorf("failed to mark URL as seen: %w", err)
	}
	return n == 1, nil // n=1 means it was added, n=0 means it was already present
}

// SaveResult serializes a document and pushes it to the results list.
func (q *Queue) SaveResult(ctx context.Context, doc ScrapedDoc) error {
	data, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("failed to serialize document: %w", err)
	}
	return q.client.LPush(ctx, ResultsKey, data).Err()
}

// QueueLength returns the number of pending URLs in the queue.
func (q *Queue) QueueLength(ctx context.Context) (int64, error) {
	return q.client.LLen(ctx, QueueKey).Result()
}

// Close the underlying connection.
func (q *Queue) Close() error {
	return q.client.Close()
}