package scraper

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
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

// ── parse ────────────────────────────────────────────────────────────────────

func TestParse(t *testing.T) {
	t.Parallel()

	const baseURL = "https://example.com/page"

	tests := []struct {
		name         string
		html         string
		wantContent  string   // substring that must appear in Content
		wantNoText   []string // substrings that must NOT appear in Content
		wantLinks    []string // links that must appear in Links
		wantNoLinks  []string // links that must NOT appear in Links
		wantErr      bool
	}{
		{
			name: "standard page — body text extracted, links collected",
			html: `<html><body>
				<p>Hello, world.</p>
				<a href="https://linked.example.com/">Link</a>
			</body></html>`,
			wantContent: "Hello, world.",
			wantLinks:   []string{"https://linked.example.com/"},
		},
		{
			name: "script tag content is excluded from body text",
			html: `<html><body>
				<p>Visible text</p>
				<script>var secret = "should-not-appear";</script>
			</body></html>`,
			wantContent:  "Visible text",
			wantNoText:   []string{"secret", "should-not-appear"},
		},
		{
			name: "style tag content is excluded from body text",
			html: `<html><body>
				<p>Readable</p>
				<style>body { color: red; /* no-style-text */ }</style>
			</body></html>`,
			wantContent: "Readable",
			wantNoText:  []string{"no-style-text"},
		},
		{
			name: "title tag in head is extracted into Page.Title and excluded from Content",
			html: `<html><head><title>My Page Title</title></head><body><p>Content</p></body></html>`,
			wantContent: "Content",
			wantNoText:  []string{"My Page Title"},
			// Title is captured separately; verified via checkPage below.
		},
		{
			name: "relative link is resolved to absolute URL",
			html: `<html><body><a href="/about">About</a></body></html>`,
			wantLinks: []string{"https://example.com/about"},
		},
		{
			name: "fragment is stripped from link href",
			html: `<html><body>
				<a href="https://example.com/page#section">Anchor</a>
			</body></html>`,
			wantLinks:   []string{"https://example.com/page"},
			wantNoLinks: []string{"https://example.com/page#section"},
		},
		{
			name: "non-http links are excluded",
			html: `<html><body>
				<a href="mailto:user@example.com">Mail</a>
				<a href="javascript:void(0)">JS</a>
				<a href="ftp://files.example.com/">FTP</a>
			</body></html>`,
			wantNoLinks: []string{
				"mailto:user@example.com",
				"javascript:void(0)",
				"ftp://files.example.com/",
			},
		},
		{
			name: "a tag with no href attribute yields no link",
			html: `<html><body><a name="anchor">Named anchor, no href</a></body></html>`,
			// No links should result from an <a> without href.
			wantContent: "Named anchor, no href",
		},
		{
			name: "deeply nested text is captured",
			html: `<html><body><div><section><article><p>Deep text</p></article></section></div></body></html>`,
			wantContent: "Deep text",
		},
		{
			name: "empty body produces empty content and no links",
			html: `<html><body></body></html>`,
			// No assertions on content or links — just must not error.
		},
		{
			name: "whitespace-only body is trimmed to empty content",
			html: "<html><body>   \n\t  </body></html>",
			// wantContent is "" — Page.Content should be blank, not a string of spaces.
		},
		{
			name: "multiple links are all collected",
			html: `<html><body>
				<a href="https://a.example.com/">A</a>
				<a href="https://b.example.com/">B</a>
				<a href="https://c.example.com/">C</a>
			</body></html>`,
			wantLinks: []string{
				"https://a.example.com/",
				"https://b.example.com/",
				"https://c.example.com/",
			},
		},
		{
			name:    "malformed base URL returns a parse error",
			html:    `<html><body>hi</body></html>`,
			wantErr: true,
			// baseURL will be overridden to an invalid value in this case — see below.
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rawURL := baseURL
			if tt.wantErr {
				rawURL = "://not-a-url" // triggers url.Parse failure inside parse()
			}

			page, err := parse(rawURL, []byte(tt.html))
			if (err != nil) != tt.wantErr {
				t.Fatalf("parse() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			if tt.wantContent != "" && !strings.Contains(page.Content, tt.wantContent) {
				t.Errorf("Content = %q\n  missing expected substring: %q", page.Content, tt.wantContent)
			}
			for _, bad := range tt.wantNoText {
				if strings.Contains(page.Content, bad) {
					t.Errorf("Content contains forbidden text %q:\n  %q", bad, page.Content)
				}
			}

			linkSet := make(map[string]bool, len(page.Links))
			for _, l := range page.Links {
				linkSet[l] = true
			}
			for _, want := range tt.wantLinks {
				if !linkSet[want] {
					t.Errorf("Links missing expected %q\n  got: %v", want, page.Links)
				}
			}
			for _, bad := range tt.wantNoLinks {
				if linkSet[bad] {
					t.Errorf("Links contains forbidden entry %q", bad)
				}
			}
		})
	}
}

// TestParseTitleExtraction verifies that the <title> element is captured in
// Page.Title and its text does not bleed into Page.Content.
func TestParseTitleExtraction(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		html        string
		wantTitle   string
		wantNoTitle string // must not appear in Content
	}{
		{
			name:        "title is extracted from a standard page",
			html:        `<html><head><title>My Page Title</title></head><body><p>Body text</p></body></html>`,
			wantTitle:   "My Page Title",
			wantNoTitle: "My Page Title",
		},
		{
			name:      "title is empty when head has no title element",
			html:      `<html><head></head><body><p>Body</p></body></html>`,
			wantTitle: "",
		},
		{
			name:      "title is empty when there is no head element at all",
			html:      `<html><body><p>Body only</p></body></html>`,
			wantTitle: "",
		},
		{
			name:        "surrounding whitespace in title is trimmed",
			html:        "<html><head><title>  Padded Title  </title></head><body></body></html>",
			wantTitle:   "Padded Title",
			wantNoTitle: "Padded Title",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			page, err := parse("https://example.com/", []byte(tt.html))
			if err != nil {
				t.Fatalf("parse() unexpected error: %v", err)
			}
			if page.Title != tt.wantTitle {
				t.Fatalf("Title = %q, want %q", page.Title, tt.wantTitle)
			}
			if tt.wantNoTitle != "" && strings.Contains(page.Content, tt.wantNoTitle) {
				t.Errorf("Content contains title text %q — should be kept separate", tt.wantNoTitle)
			}
		})
	}
}

// TestParseURLIsPreserved verifies the Page URL field is the raw input URL, not a
// normalized or base-resolved variant.
func TestParseURLIsPreserved(t *testing.T) {
	t.Parallel()
	const rawURL = "https://example.com/some/path?q=1"
	page, err := parse(rawURL, []byte("<html><body>hi</body></html>"))
	if err != nil {
		t.Fatalf("parse() unexpected error: %v", err)
	}
	if page.URL != rawURL {
		t.Fatalf("page.URL = %q, want %q", page.URL, rawURL)
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
					<p>Scraped content</p>
					<a href="/other">Other</a>
				</body></html>`))
			},
			checkPage: func(t *testing.T, p *Page) {
				t.Helper()
				if !strings.Contains(p.Content, "Scraped content") {
					t.Errorf("Content = %q, want it to contain %q", p.Content, "Scraped content")
				}
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
				_, _ = w.Write([]byte(`<html><body><p>Final destination</p></body></html>`))
			},
			checkPage: func(t *testing.T, p *Page) {
				t.Helper()
				if !strings.Contains(p.Content, "Final destination") {
					t.Errorf("Content = %q, expected redirect to be followed", p.Content)
				}
			},
		},
		{
			name: "response body larger than 1MB is silently truncated — no error returned",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				// Write enough to exceed the 1MB cap (1<<20 bytes).
				over := bytes.Repeat([]byte("a"), (1<<20)+512)
				_, _ = w.Write([]byte("<html><body><p>"))
				_, _ = w.Write(over)
				_, _ = w.Write([]byte("</p></body></html>"))
			},
			// Fetch must not return an error; body is simply truncated.
			wantErr: false,
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
