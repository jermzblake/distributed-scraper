package worker

import (
	"strings"
	"testing"

	"distributed-scraper/scraper"
)

func TestContainsHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		rawURL string
		host   string
		want   bool
	}{
		{
			name:   "matches https host",
			rawURL: "https://books.toscrape.com/catalogue/page-1.html",
			host:   "books.toscrape.com",
			want:   true,
		},
		{
			name:   "matches http host",
			rawURL: "http://books.toscrape.com/",
			host:   "books.toscrape.com",
			want:   true,
		},
		{
			name:   "matches host case-insensitively",
			rawURL: "https://Books.ToScrape.com/",
			host:   "books.toscrape.com",
			want:   true,
		},
		{
			name:   "does not match different host",
			rawURL: "https://quotes.toscrape.com/",
			host:   "books.toscrape.com",
			want:   false,
		},
		{
			name:   "does not match relative URL",
			rawURL: "/catalogue/page-1.html",
			host:   "books.toscrape.com",
			want:   false,
		},
		{
			name:   "does not panic or match short URL",
			rawURL: "https://short.example/",
			host:   "this-host-name-is-longer-than-url",
			want:   false,
		},
		{
			name:   "does not match malformed URL",
			rawURL: "://bad-url",
			host:   "books.toscrape.com",
			want:   false,
		},
		{
			name:   "does not match empty URL",
			rawURL: "",
			host:   "books.toscrape.com",
			want:   false,
		},
		{
			name:   "does not match empty host",
			rawURL: "https://books.toscrape.com/",
			host:   "",
			want:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := ContainsHost(tt.rawURL, tt.host)
			if got != tt.want {
				t.Fatalf("ContainsHost(%q, %q) = %v, want %v", tt.rawURL, tt.host, got, tt.want)
			}
		})
	}
}

// ── shouldFollow ──────────────────────────────────────────────────────────────

func TestShouldFollow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		allowedHost string // empty means "allow all"
		link        string
		want        bool
	}{
		{
			name:        "no host restriction — any URL is followed",
			allowedHost: "",
			link:        "https://anything.example.com/page",
			want:        true,
		},
		{
			name:        "link matches the allowed host exactly — followed",
			allowedHost: "books.toscrape.com",
			link:        "https://books.toscrape.com/catalogue/page-1.html",
			want:        true,
		},
		{
			name:        "link is on a different host — blocked",
			allowedHost: "books.toscrape.com",
			link:        "https://quotes.toscrape.com/page/1/",
			want:        false,
		},
		{
			name:        "subdomain of allowed host is blocked — host match is exact, not suffix",
			allowedHost: "toscrape.com",
			link:        "https://books.toscrape.com/",
			want:        false,
		},
		{
			name:        "malformed URL is blocked when a host restriction is set",
			allowedHost: "books.toscrape.com",
			link:        "://not-a-url",
			want:        false,
		},
		{
			name:        "relative URL is blocked when a host restriction is set",
			allowedHost: "books.toscrape.com",
			link:        "/catalogue/index.html",
			want:        false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			w := &Worker{cfg: Config{AllowedHost: tt.allowedHost}}
			got := w.shouldFollow(tt.link)
			if got != tt.want {
				t.Fatalf("shouldFollow(%q) with AllowedHost=%q = %v, want %v",
					tt.link, tt.allowedHost, got, tt.want)
			}
		})
	}
}

// ── NewWorker ─────────────────────────────────────────────────────────────────

func TestNewWorker(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		redisAddr   string
		wantErr     bool
		errContains string
	}{
		{
			name:        "unreachable Redis address returns an error containing the address",
			redisAddr:   "localhost:1", // port 1 is never open
			wantErr:     true,
			errContains: "localhost:1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := Config{
				ID:        "test-worker",
				RedisAddr: tt.redisAddr,
			}
			w, err := NewWorker(cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("NewWorker() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if w == nil {
				t.Fatal("NewWorker() returned nil Worker, want non-nil")
			}
		})
	}
}

func TestBuildEnrichedChunk(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		page      *scraper.Page
		chunkText string
		wantParts []string
		noParts   []string
	}{
		{
			name: "includes title description and chunk text when present",
			page: &scraper.Page{
				Title:       "Intro to Crawling",
				Description: "A practical guide",
			},
			chunkText: "This is the chunk body.",
			wantParts: []string{"Title: Intro to Crawling", "A practical guide", "This is the chunk body."},
		},
		{
			name: "omits empty title and description",
			page: &scraper.Page{},
			chunkText: "Only raw chunk content.",
			wantParts: []string{"Only raw chunk content."},
			noParts:   []string{"Title:"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := buildEnrichedChunk(tt.page, tt.chunkText)
			for _, part := range tt.wantParts {
				if !strings.Contains(got, part) {
					t.Fatalf("buildEnrichedChunk() output missing %q\n  got: %q", part, got)
				}
			}
			for _, part := range tt.noParts {
				if strings.Contains(got, part) {
					t.Fatalf("buildEnrichedChunk() output unexpectedly contains %q\n  got: %q", part, got)
				}
			}
		})
	}
}
