package scraper

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/html"
)

type Page struct {
	URL string
	Title string
	Content string
	Links []string
}

var httpClient = &http.Client{
	Timeout: 15 * time.Second,
}

// Fetch downloads a URL and parses its title, text content, and links.
func Fetch(ctx context.Context, rawUrl string) (*Page, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", rawUrl, nil)

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP get %s error: %w", rawUrl, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP get %s returned status %d", rawUrl, resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))	// Limit to 1MB
	if err != nil {
		return nil, fmt.Errorf("read body error: %w", err)
	}

	return parse(rawUrl, body)
}

func parse(rawUrl string, body []byte) (*Page, error) {
	base, err := url.Parse(rawUrl)
	if err != nil {
		return nil, fmt.Errorf("parse URL error: %w", err)
	}

	doc, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, fmt.Errorf("HTML parse error: %w", err)
	}

	page := &Page{URL: rawUrl}
	var sb strings.Builder

	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "script", "style", "head":
				return // Skip non-content elements
			case "a":
				for _, attr := range n.Attr {
					if attr.Key == "href" {
						if resolved := resolveURL(base, attr.Val); resolved != "" {
							page.Links = append(page.Links, resolved)
						}
					}
				}
			}
		}
		if n.Type == html.TextNode {
			text := strings.TrimSpace(n.Data)
			if text != "" {
				sb.WriteString(text)
				sb.WriteByte(' ')
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)

	page.Content = strings.TrimSpace(sb.String())
	return page, nil
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