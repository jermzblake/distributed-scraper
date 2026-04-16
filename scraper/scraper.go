package scraper

import (
	"context"
	"fmt"

	// "io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

type Page struct {
	URL string
	Title string
	Description string 	// meta description — great for vector metadata
	Headings    []string	
	Content string			// clean body text only
	Links []string
}

var httpClient = &http.Client{
	Timeout: 15 * time.Second,
}

// Fetch downloads a URL and parses its title, text content, and links.
func Fetch(ctx context.Context, rawUrl string) (*Page, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawUrl, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; go-scraper/1.0)")

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP get %s error: %w", rawUrl, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP get %s returned status %d", rawUrl, resp.StatusCode)
	}

	// goquery.NewDocumentFromReader parses HTML from any io.Reader.
	// It handles malformed HTML gracefully (same as a browser would).
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("parse html: %w", err)
	}

	base, _ := url.Parse(rawUrl)
	return extract(doc, base), nil
}

func extract(doc *goquery.Document, base *url.URL) *Page {
	page := &Page{URL: base.String()}
	page.Title = strings.TrimSpace(doc.Find("title").First().Text())
	// The meta description is often the best single-sentence summary of a page.
	// Including it as metadata in Qdrant lets you display it in search results.
	page.Description, _ = doc.Find("meta[name='description']").Attr("content")

	doc.Find("h1, h2, h3").Each(func(_ int, s *goquery.Selection) {
		if text := strings.TrimSpace(s.Text()); text != "" {
			page.Headings = append(page.Headings, text)
		}
	})

	// First pass: remove universally noisy elements	
	doc.Find(`
    script, style, nav, footer, header, aside,
    form, button, input, select,
    .cookie-banner, #cookie-notice,
    [aria-hidden="true"]
	`).Remove()

	// Second pass: remove site-specific noise.
	// These selectors are specific to books.toscrape.com but the pattern
	// applies everywhere — you'll add site-specific rules as you encounter them.
	doc.Find(`
    .sidebar,
    .side_categories,
    .promotions,
    .alert,
    .breadcrumb
	`).Remove()

	// Prefer semantic content containers; fall back to body.
	var contentSel *goquery.Selection
	for _, selector := range []string{"article", "main", "[role='main']", ".content", "#content", "body"} {
		if sel := doc.Find(selector); sel.Length() > 0 {
			contentSel = sel.First()
			break
		}
	}

	if contentSel != nil {
		var parts []string
		// Extract paragraph and heading text with spacing between them.
		// This produces more natural text than concatenating all TextNodes.
		contentSel.Find("p, h1, h2, h3, h4, li").Each(func(_ int, s *goquery.Selection) {
			if text := strings.TrimSpace(s.Text()); text != "" {
				// Additional guard: skip very short strings — they're usually
				// button labels, price strings, or single-word nav items.
				if len(text) > 40 {
						parts = append(parts, text)
				}
			}
		})
		page.Content = strings.Join(parts, "\n\n")
	}

	doc.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		href, _ := s.Attr("href")
		if resolved := resolveURL(base, href); resolved != "" {
			page.Links = append(page.Links, resolved)
		}
	})

	return page

}

// resolveURL resolves a possibly relative URL against the base URL and returns an absolute URL string.
func resolveURL(base *url.URL, href string) string {
	parsed, err := url.Parse(href)
	if err != nil {
		return ""
	}

	resolved := base.ResolveReference(parsed)
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return ""
	}
	// String fragments - #section links point to the same page
	resolved.Fragment = ""
	return resolved.String()
}