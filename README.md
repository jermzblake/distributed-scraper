# distributed-scraper

A concurrent web crawler built in Go, backed by Redis as a distributed work queue. Multiple worker processes pull URLs from a shared queue, scrape pages in parallel, and store results back to Redis — enabling horizontal scaling across machines or processes.

## Architecture

```
seed/ ──push──> Redis queue ──pop──> worker(s) ──parse──> Redis results
                   ↑                    │
                   └────── new links ───┘
```

- **`seed/`** — Pushes starting URLs into the Redis queue
- **`queue/`** — Redis-backed work queue (push, pop, dedup via seen-set, results store)
- **`scraper/`** — Fetches pages over HTTP, extracts title, text content, and links
- **`worker/`** — Pulls URLs from the queue, scrapes them, enqueues discovered links
- **`main.go`** — Worker entry point and export tool

## Usage

**Prerequisites:** Redis running on `localhost:6379`

```bash
# 1. Seed the queue
go run ./seed/

# 2. Start one or more workers (in separate terminals)
go run . --id worker-1
go run . --id worker-2

# 3. Export results to JSON
go run . --export --out results.json

# Ctrl-C to stop workers gracefully
```

### Flags

| Flag       | Default              | Description                                     |
| ---------- | -------------------- | ----------------------------------------------- |
| `--id`     | `worker-1`           | Unique worker ID                                |
| `--redis`  | `localhost:6379`     | Redis address                                   |
| `--host`   | `books.toscrape.com` | Restrict crawl to this hostname                 |
| `--export` | `false`              | Dump results to JSON and exit                   |
| `--out`    | `results.json`       | Output file for `--export` (use `-` for stdout) |
| `--reset`  | `false`              | Clear Redis results and seen-set after export   |

### Export Output Format

Results export as a JSON array of `ScrapedDoc` objects. **Note:** Results appear in **reverse insertion order** (LIFO) — the most recently scraped pages appear first. Each document includes:

```json
[
  {
    "url": "https://example.com/page",
    "title": "Page Title",
    "content": "Extracted text content",
    "links": ["https://example.com/link1", "https://example.com/link2"],
    "scraped_at": "2026-04-15T12:34:56Z",
    "worker_id": "worker-1",
    "metadata": {
      "description": "Meta description from the page"
    }
  }
]
```

## Running Tests

```bash
go test ./...
```

## Stack

- **Go 1.25**
- **Redis** via [`go-redis/v9`](https://github.com/redis/go-redis)
- [`golang.org/x/net/html`](https://pkg.go.dev/golang.org/x/net/html) for HTML parsing
- [`miniredis`](https://github.com/alicebob/miniredis) for in-memory Redis in tests
