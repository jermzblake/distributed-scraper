package scraper

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
)

// ── helpers ───────────────────────────────────────────────────────────────────

// mustParseBase parses a URL and fatals the test on failure.
func mustParseBase(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("mustParseBase(%q): %v", raw, err)
	}
	return u
}

func assertContainsAll(t *testing.T, content string, want []string, label string) {
	t.Helper()
	for _, s := range want {
		if !strings.Contains(content, s) {
			t.Errorf("%s missing expected substring %q\n  got: %q", label, s, content)
		}
	}
}

func assertContainsNone(t *testing.T, content string, forbidden []string, label string) {
	t.Helper()
	for _, s := range forbidden {
		if strings.Contains(content, s) {
			t.Errorf("%s contains forbidden substring %q\n  got: %q", label, s, content)
		}
	}
}

func toSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, v := range values {
		set[v] = true
	}
	return set
}

func assertSetContainsAll(t *testing.T, gotSet map[string]bool, want []string, label string, gotRaw []string) {
	t.Helper()
	for _, v := range want {
		if !gotSet[v] {
			t.Errorf("%s missing expected value %q\n  got: %v", label, v, gotRaw)
		}
	}
}

func assertSetContainsNone(t *testing.T, gotSet map[string]bool, forbidden []string, label string, gotRaw []string) {
	t.Helper()
	for _, v := range forbidden {
		if gotSet[v] {
			t.Errorf("%s contains forbidden value %q\n  got: %v", label, v, gotRaw)
		}
	}
}

// ── resolveURL ────────────────────────────────────────────────────────────────

func TestResolveURL(t *testing.T) {
	t.Parallel()

	base := mustParseBase(t, "https://example.com/dir/page.html")

	tests := []struct {
		name string
		href string
		want string // "" means the link should be dropped
	}{
		// Happy path — absolute URLs are returned unchanged (modulo fragment)
		{
			name: "absolute https URL is returned as-is",
			href: "https://other.example.com/about",
			want: "https://other.example.com/about",
		},
		{
			name: "absolute http URL is returned as-is",
			href: "http://other.example.com/about",
			want: "http://other.example.com/about",
		},
		// Relative URLs resolved against base
		{
			name: "path-relative URL is resolved against base directory",
			href: "sibling.html",
			want: "https://example.com/dir/sibling.html",
		},
		{
			name: "root-relative URL resolves to base origin",
			href: "/catalogue/index.html",
			want: "https://example.com/catalogue/index.html",
		},
		{
			name: "protocol-relative URL inherits base scheme",
			href: "//cdn.example.com/style.css",
			want: "https://cdn.example.com/style.css",
		},
		// Fragment stripping
		{
			name: "fragment is stripped from an absolute URL",
			href: "https://example.com/page#section",
			want: "https://example.com/page",
		},
		{
			name: "fragment-only href resolves to base URL without fragment",
			href: "#top",
			want: "https://example.com/dir/page.html",
		},
		// Non-HTTP schemes must be dropped
		{
			name: "mailto: link is dropped",
			href: "mailto:user@example.com",
			want: "",
		},
		{
			name: "javascript: link is dropped",
			href: "javascript:void(0)",
			want: "",
		},
		{
			name: "ftp: link is dropped",
			href: "ftp://files.example.com/pub",
			want: "",
		},
		// Malformed input
		{
			name: "entirely malformed href is dropped",
			href: "://bad-url",
			want: "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := resolveURL(base, tt.href)
			if got != tt.want {
				t.Fatalf("resolveURL(%q) = %q, want %q", tt.href, got, tt.want)
			}
		})
	}
}

// ── extract ─────────────────────────────────────────────────────────────────

func TestExtract(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		baseURL             string
		html                string
		wantTitle           string
		wantDescription     string
		wantHeadings        []string
		wantContentContains []string
		wantContentNot      []string
		wantLinks           []string
		wantNoLinks         []string
	}{
		{
			name:            "extracts title description headings content and normalized links",
			baseURL:         "https://example.com/catalog/page.html?ref=1",
			html:            `<html><head><title>My Page Title</title><meta name="description" content="Summary for search"></head><body><h1>Primary Heading</h1><p>This paragraph has enough characters to be retained in extracted content safely.</p><a href="/about#team">About</a></body></html>`,
			wantTitle:       "My Page Title",
			wantDescription: "Summary for search",
			wantHeadings:    []string{"Primary Heading"},
			wantContentContains: []string{
				"This paragraph has enough characters to be retained in extracted content safely.",
			},
			wantLinks: []string{"https://example.com/about"},
		},
		{
			name:            "removes noisy elements and excludes short snippets",
			baseURL:         "https://example.com/",
			html:            `<html><body><nav><p>This navigation paragraph is intentionally long enough to be excluded by selector removal.</p></nav><p>short text</p><p>This visible paragraph is intentionally longer than forty characters and should survive.</p><script>var secret = "do-not-include";</script></body></html>`,
			wantContentContains: []string{
				"This visible paragraph is intentionally longer than forty characters and should survive.",
			},
			wantContentNot: []string{
				"navigation paragraph",
				"do-not-include",
				"short text",
			},
		},
		{
			name:            "prefers article over body when selecting content root",
			baseURL:         "https://example.com/",
			html:            `<html><body><p>This body paragraph is long but should not be included when article exists for content extraction.</p><article><p>This article paragraph is long enough and should be chosen as the content source.</p></article></body></html>`,
			wantContentContains: []string{
				"This article paragraph is long enough and should be chosen as the content source.",
			},
			wantContentNot: []string{
				"This body paragraph is long but should not be included",
			},
		},
		{
			name:        "filters non-http links and keeps absolute https links",
			baseURL:     "https://example.com/path",
			html:        `<html><body><a href="mailto:user@example.com">mail</a><a href="ftp://files.example.com/pub">ftp</a><a href="https://other.example.com/page">https</a></body></html>`,
			wantLinks:   []string{"https://other.example.com/page"},
			wantNoLinks: []string{"mailto:user@example.com", "ftp://files.example.com/pub"},
		},
		{
			name:     "page URL is preserved from base URL",
			baseURL:  "https://example.com/some/path?q=1",
			html:     `<html><body><p>This content string exceeds forty characters so URL assertion is exercised with valid content.</p></body></html>`,
			wantLinks: nil,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			doc, err := goquery.NewDocumentFromReader(strings.NewReader(tt.html))
			if err != nil {
				t.Fatalf("goquery.NewDocumentFromReader() unexpected error: %v", err)
			}

			base := mustParseBase(t, tt.baseURL)
			page := extract(doc, base)

			if page.URL != tt.baseURL {
				t.Fatalf("page.URL = %q, want %q", page.URL, tt.baseURL)
			}
			if page.Title != tt.wantTitle {
				t.Fatalf("Title = %q, want %q", page.Title, tt.wantTitle)
			}
			if page.Description != tt.wantDescription {
				t.Fatalf("Description = %q, want %q", page.Description, tt.wantDescription)
			}

			headingSet := toSet(page.Headings)
			assertSetContainsAll(t, headingSet, tt.wantHeadings, "Headings", page.Headings)

			assertContainsAll(t, page.Content, tt.wantContentContains, "Content")
			assertContainsNone(t, page.Content, tt.wantContentNot, "Content")

			linkSet := toSet(page.Links)
			assertSetContainsAll(t, linkSet, tt.wantLinks, "Links", page.Links)
			assertSetContainsNone(t, linkSet, tt.wantNoLinks, "Links", page.Links)
		})
	}
}

// ── Fetch ────────────────────────────────────────────────────────────────────

func TestFetch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		handler    http.HandlerFunc
		wantErr    bool
		errContains string // substring the error message must contain
		checkPage  func(t *testing.T, p *Page)
	}{
		{
			name: "200 response with valid HTML returns a populated Page",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`<html><body>
					<p>This paragraph is intentionally longer than forty characters so content extraction keeps it.</p>
					<a href="/other">Other</a>
				</body></html>`))
			},
			checkPage: func(t *testing.T, p *Page) {
				t.Helper()
				assertContainsAll(t, p.Content, []string{"intentionally longer than forty characters"}, "Content")
				if len(p.Links) == 0 {
					t.Error("Links is empty, want at least one link")
				}
			},
		},
		{
			name: "404 response returns an error containing the status code",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "Not Found", http.StatusNotFound)
			},
			wantErr:     true,
			errContains: "404",
		},
		{
			name: "500 response returns an error containing the status code",
			handler: func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			},
			wantErr:     true,
			errContains: "500",
		},
		{
			name: "301 redirect is followed and final page is returned",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/" {
					http.Redirect(w, r, "/final", http.StatusMovedPermanently)
					return
				}
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`<html><body><p>This final destination paragraph is long enough to remain in extracted content.</p></body></html>`))
			},
			checkPage: func(t *testing.T, p *Page) {
				t.Helper()
				assertContainsAll(t, p.Content, []string{"final destination paragraph"}, "Content")
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			page, err := Fetch(context.Background(), srv.URL)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Fetch() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Fatalf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				return
			}
			if tt.checkPage != nil {
				tt.checkPage(t, page)
			}
		})
	}
}

// TestFetchCancelledContext verifies that a pre-cancelled context is propagated
// to the HTTP request and causes Fetch to return an error immediately.
func TestFetchCancelledContext(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html><body>ok</body></html>"))
	}))
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err := Fetch(ctx, srv.URL)
	if err == nil {
		t.Fatal("Fetch() with cancelled context returned nil error, want an error")
	}
}

// TestFetchServerUnavailable verifies that a network-level failure (e.g. the
// server is not running) returns an error.
func TestFetchServerUnavailable(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	addr := srv.URL
	srv.Close() // shut down before calling Fetch

	_, err := Fetch(context.Background(), addr)
	if err == nil {
		t.Fatal("Fetch() against closed server returned nil error, want an error")
	}
}
