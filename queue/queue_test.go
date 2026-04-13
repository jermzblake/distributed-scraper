package queue_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jermzblake/distributed-scraper/queue"
)

// newTestQueue spins up an in-memory Redis and returns a Queue pointed at it.
// The server is automatically closed when t finishes.
func newTestQueue(t *testing.T) (*queue.Queue, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	return queue.New(mr.Addr()), mr
}

// sampleDoc returns a fully-populated ScrapedDoc for use in tests.
func sampleDoc(url string) queue.ScrapedDoc {
	return queue.ScrapedDoc{
		URL:       url,
		Title:     "Test Page",
		Content:   "Hello, world.",
		Links:     []string{"https://linked.example/"},
		ScrapedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		WorkerID:  "worker-test-1",
		Metadata:  map[string]string{"env": "test"},
	}
}

// ── Ping ─────────────────────────────────────────────────────────────────────

func TestPing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stopFirst bool
		wantErr   bool
	}{
		{
			name:    "succeeds against a running server",
			wantErr: false,
		},
		{
			name:      "fails when the server has been stopped",
			stopFirst: true,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, mr := newTestQueue(t)
			if tt.stopFirst {
				mr.Close()
			}
			err := q.Ping(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// ── Push / Pop ────────────────────────────────────────────────────────────────

func TestPushAndPop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		push      []string
		wantPops  []string
		wantEmpty bool // expect empty string (queue drained / timed out) on Pop
	}{
		{
			name:     "single URL is returned after push",
			push:     []string{"https://example.com"},
			wantPops: []string{"https://example.com"},
		},
		{
			name:     "multiple pushes are popped in FIFO order",
			push:     []string{"https://first.com", "https://second.com", "https://third.com"},
			wantPops: []string{"https://first.com", "https://second.com", "https://third.com"},
		},
		{
			name:      "pop on empty queue returns empty string without error",
			push:      nil,
			wantEmpty: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, _ := newTestQueue(t)
			ctx := context.Background()

			for _, u := range tt.push {
				if err := q.Push(ctx, u); err != nil {
					t.Fatalf("Push(%q) unexpected error: %v", u, err)
				}
			}

			if tt.wantEmpty {
				// go-redis enforces a minimum BRPOP timeout of 1s; anything shorter is truncated.
				got, err := q.Pop(ctx, time.Second)
				if err != nil {
					t.Fatalf("Pop() on empty queue unexpected error: %v", err)
				}
				if got != "" {
					t.Fatalf("Pop() on empty queue = %q, want empty string", got)
				}
				return
			}

			for i, want := range tt.wantPops {
				got, err := q.Pop(ctx, time.Second)
				if err != nil {
					t.Fatalf("Pop()[%d] unexpected error: %v", i, err)
				}
				if got != want {
					t.Fatalf("Pop()[%d] = %q, want %q", i, got, want)
				}
			}
		})
	}
}

// ── MarkSeen ─────────────────────────────────────────────────────────────────

func TestMarkSeen(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		seed    []string // URLs to mark seen before the assertion
		testURL string
		want    bool
	}{
		{
			name:    "returns true for a first-seen URL",
			testURL: "https://new.example.com",
			want:    true,
		},
		{
			name:    "returns false for a URL seen a second time",
			seed:    []string{"https://dup.example.com"},
			testURL: "https://dup.example.com",
			want:    false,
		},
		{
			name:    "different URLs are tracked independently — second URL still returns true",
			seed:    []string{"https://a.example.com"},
			testURL: "https://b.example.com",
			want:    true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, _ := newTestQueue(t)
			ctx := context.Background()

			for _, u := range tt.seed {
				if _, err := q.MarkSeen(ctx, u); err != nil {
					t.Fatalf("MarkSeen(%q) seed: %v", u, err)
				}
			}

			got, err := q.MarkSeen(ctx, tt.testURL)
			if err != nil {
				t.Fatalf("MarkSeen(%q) unexpected error: %v", tt.testURL, err)
			}
			if got != tt.want {
				t.Fatalf("MarkSeen(%q) = %v, want %v", tt.testURL, got, tt.want)
			}
		})
	}
}

// ── SaveResult / ExportResults ────────────────────────────────────────────────

func TestExportResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		docs      []queue.ScrapedDoc
		setupMR   func(mr *miniredis.Miniredis) // inject raw bytes directly into Redis
		wantErr   bool
		wantCount int
		checkURL  string // if set, verify this URL appears in the export
	}{
		{
			name:      "empty results list exports a valid empty JSON array",
			wantErr:   false,
			wantCount: 0,
		},
		{
			name:      "single saved doc survives a full serialization roundtrip",
			docs:      []queue.ScrapedDoc{sampleDoc("https://single.example.com")},
			wantErr:   false,
			wantCount: 1,
			checkURL:  "https://single.example.com",
		},
		{
			name: "multiple saved docs are all present in the export",
			docs: []queue.ScrapedDoc{
				sampleDoc("https://page1.example.com"),
				sampleDoc("https://page2.example.com"),
				sampleDoc("https://page3.example.com"),
			},
			wantErr:   false,
			wantCount: 3,
		},
		{
			name: "corrupted entry in the results list returns a decode error",
			setupMR: func(mr *miniredis.Miniredis) {
				if _, err := mr.Lpush(queue.ResultsKey, "{{not-valid-json}}"); err != nil {
					// Lpush failure is a test setup problem, not the behaviour under test.
					panic(fmt.Sprintf("miniredis Lpush: %v", err))
				}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, mr := newTestQueue(t)
			ctx := context.Background()

			for _, doc := range tt.docs {
				if err := q.SaveResult(ctx, doc); err != nil {
					t.Fatalf("SaveResult() unexpected error: %v", err)
				}
			}
			if tt.setupMR != nil {
				tt.setupMR(mr)
			}

			var buf bytes.Buffer
			err := q.ExportResults(ctx, &buf)
			if (err != nil) != tt.wantErr {
				t.Fatalf("ExportResults() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			var got []queue.ScrapedDoc
			if err := json.Unmarshal(buf.Bytes(), &got); err != nil {
				t.Fatalf("failed to unmarshal ExportResults output: %v\nraw output: %s", err, buf.String())
			}
			if len(got) != tt.wantCount {
				t.Fatalf("ExportResults() returned %d docs, want %d", len(got), tt.wantCount)
			}
			if tt.checkURL != "" {
				found := false
				for _, d := range got {
					if d.URL == tt.checkURL {
						found = true
						// Verify key fields are preserved across the roundtrip.
						if d.WorkerID != "worker-test-1" {
							t.Errorf("doc.WorkerID = %q, want %q", d.WorkerID, "worker-test-1")
						}
						if d.Title != "Test Page" {
							t.Errorf("doc.Title = %q, want %q", d.Title, "Test Page")
						}
					}
				}
				if !found {
					t.Fatalf("ExportResults() output missing expected URL %q", tt.checkURL)
				}
			}
		})
	}
}

// ── QueueLength ───────────────────────────────────────────────────────────────

func TestQueueLength(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		push []string
		want int64
	}{
		{
			name: "returns zero on an empty queue",
			want: 0,
		},
		{
			name: "returns one after a single push",
			push: []string{"https://example.com"},
			want: 1,
		},
		{
			name: "returns three after three pushes",
			push: []string{"https://a.com", "https://b.com", "https://c.com"},
			want: 3,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, _ := newTestQueue(t)
			ctx := context.Background()

			for _, u := range tt.push {
				if err := q.Push(ctx, u); err != nil {
					t.Fatalf("Push(%q) unexpected error: %v", u, err)
				}
			}

			got, err := q.QueueLength(ctx)
			if err != nil {
				t.Fatalf("QueueLength() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("QueueLength() = %d, want %d", got, tt.want)
			}
		})
	}
}

// ── ResultsCount ──────────────────────────────────────────────────────────────

func TestResultsCount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		saves int
		want  int64
	}{
		{
			name: "returns zero when no results have been saved",
			want: 0,
		},
		{
			name:  "returns one after a single save",
			saves: 1,
			want:  1,
		},
		{
			name:  "returns the correct count after multiple saves",
			saves: 5,
			want:  5,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, _ := newTestQueue(t)
			ctx := context.Background()

			for i := 0; i < tt.saves; i++ {
				url := fmt.Sprintf("https://example.com/page-%d", i)
				if err := q.SaveResult(ctx, sampleDoc(url)); err != nil {
					t.Fatalf("SaveResult(%q) unexpected error: %v", url, err)
				}
			}

			got, err := q.ResultsCount(ctx)
			if err != nil {
				t.Fatalf("ResultsCount() unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("ResultsCount() = %d, want %d", got, tt.want)
			}
		})
	}
}

// ── Reset ─────────────────────────────────────────────────────────────────────

func TestReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		setupResultCount   int    // save this many results before Reset
		setupSeenURL       string // mark this URL seen before Reset
		setupQueueURLs     []string
		checkResultsEmpty  bool   // assert ResultsCount == 0 after Reset
		checkSeenCleared   string // assert this URL returns true from MarkSeen after Reset
		checkQueuePreserved bool  // assert QueueLength is unchanged after Reset
	}{
		{
			name:              "results list is cleared by Reset",
			setupResultCount:  3,
			checkResultsEmpty: true,
		},
		{
			name:             "seen set is cleared — previously-seen URL returns true again after Reset",
			setupSeenURL:     "https://was-seen.example.com",
			checkSeenCleared: "https://was-seen.example.com",
		},
		{
			name:                "work queue is not touched by Reset",
			setupQueueURLs:      []string{"https://pending.example.com"},
			checkQueuePreserved: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			q, _ := newTestQueue(t)
			ctx := context.Background()

			for i := 0; i < tt.setupResultCount; i++ {
				url := fmt.Sprintf("https://result%d.example.com", i)
				if err := q.SaveResult(ctx, sampleDoc(url)); err != nil {
					t.Fatalf("SaveResult() setup error: %v", err)
				}
			}
			if tt.setupSeenURL != "" {
				if _, err := q.MarkSeen(ctx, tt.setupSeenURL); err != nil {
					t.Fatalf("MarkSeen() setup error: %v", err)
				}
			}
			for _, u := range tt.setupQueueURLs {
				if err := q.Push(ctx, u); err != nil {
					t.Fatalf("Push() setup error: %v", err)
				}
			}

			if err := q.Reset(ctx); err != nil {
				t.Fatalf("Reset() unexpected error: %v", err)
			}

			if tt.checkResultsEmpty {
				count, err := q.ResultsCount(ctx)
				if err != nil {
					t.Fatalf("ResultsCount() post-Reset: %v", err)
				}
				if count != 0 {
					t.Fatalf("ResultsCount() after Reset = %d, want 0", count)
				}
			}
			if tt.checkSeenCleared != "" {
				added, err := q.MarkSeen(ctx, tt.checkSeenCleared)
				if err != nil {
					t.Fatalf("MarkSeen() post-Reset: %v", err)
				}
				if !added {
					t.Fatalf("MarkSeen(%q) after Reset = false, want true (seen set should have been cleared)", tt.checkSeenCleared)
				}
			}
			if tt.checkQueuePreserved {
				length, err := q.QueueLength(ctx)
				if err != nil {
					t.Fatalf("QueueLength() post-Reset: %v", err)
				}
				if length != int64(len(tt.setupQueueURLs)) {
					t.Fatalf("QueueLength() after Reset = %d, want %d (queue should not be cleared)", length, len(tt.setupQueueURLs))
				}
			}
		})
	}
}
