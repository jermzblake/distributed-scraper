# distributed-scraper

A concurrent web crawler and semantic search pipeline built in Go.

Workers pull URLs from Redis, scrape and clean page content, split content into overlapping chunks, embed chunks with Ollama, and index vectors in Qdrant for retrieval. Scraped documents are still stored in Redis for export/debug workflows.

## Architecture

```
seed/ -> Redis queue -> worker(s)
       |- scrape HTML (title, description, headings, body, links)
       |- chunk text (overlap)
       |- embed chunks (Ollama)
       |- upsert vectors (Qdrant)
       '- save full docs + metadata (Redis results)

     discovered links -----------------------------> Redis queue
```

- **`seed/`** — Pushes starting URLs into the Redis queue
- **`queue/`** — Redis-backed queue + seen-set dedup + results store + export/reset helpers
- **`scraper/`** — HTTP fetch + goquery parsing (title, meta description, headings, cleaned content, links)
- **`chunker/`** — Overlapping word-based chunking for retrieval quality
- **`embedder/`** — Ollama embed client with batching and retrieval instruction prefixes
- **`vectorstore/`** — Qdrant gRPC client (collection bootstrap, upsert, vector search)
- **`worker/`** — End-to-end ingest loop (dedup, rate limit, scrape, chunk, embed, index, enqueue links)
- **`main.go`** — Worker runtime + results export mode
- **`cmd/search/`** — Query-time embedding + top-k nearest-neighbor search against Qdrant

## Usage

### Prerequisites

- Redis on `localhost:6379`
- Qdrant gRPC on `localhost:6334`
- Ollama on `http://localhost:11434` with an embedding model available

Quick start with Docker + Ollama model pull:

```bash
docker compose up -d
ollama pull qwen3-embedding:0.6b
```

### Crawl + Index

```bash
# 1. Seed the queue
go run ./seed/

# 2. Start one or more workers (in separate terminals)
go run . --id worker-1 --host books.toscrape.com
go run . --id worker-2 --host books.toscrape.com

# 3. (Optional) export Redis-stored scraped docs to JSON
go run . --export --out results.json

# Ctrl-C to stop workers gracefully
```

### Semantic Search

```bash
# Default query
go run ./cmd/search

# Custom query
go run ./cmd/search "mystery books with good reviews"
```

`cmd/search` embeds the query and returns top 5 matches from Qdrant, including score, URL, title, and a text preview.

Set `OLLAMA_ADDR` to override the Ollama endpoint used by `cmd/search` (default falls back to `http://localhost:11434`).

### Flags

| Flag       | Default                  | Description                                     |
| ---------- | ------------------------ | ----------------------------------------------- |
| `--id`     | `worker-1`               | Unique worker ID                                |
| `--redis`  | `localhost:6379`         | Redis address                                   |
| `--qdrant` | `localhost:6334`         | Qdrant gRPC address                             |
| `--host`   | `books.toscrape.com`     | Allowed host for discovered links               |
| `--rate`   | `2.0`                    | Per-domain requests/sec                         |
| `--burst`  | `5`                      | Per-domain token bucket burst                   |
| `--ollama` | `http://localhost:11434` | Ollama base URL                                 |
| `--model`  | `qwen3-embedding:0.6b`   | Embedding model                                 |
| `--dims`   | `1024`                   | Embedding dimension (must match model)          |
| `--export` | `false`                  | Dump Redis results to JSON and exit             |
| `--out`    | `results.json`           | Output file for `--export` (use `-` for stdout) |
| `--reset`  | `false`                  | Clear Redis results and seen-set after export   |

Notes:

- `--reset` is only meaningful with `--export`.
- `seed/` includes both Books and Quotes seeds, but with default `--host books.toscrape.com`, discovered non-Books links are filtered out.

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
- **Qdrant** via [`qdrant/go-client`](https://github.com/qdrant/go-client) (gRPC)
- **Ollama** for local embeddings
- [`goquery`](https://github.com/PuerkitoBio/goquery) for HTML parsing and extraction
- [`miniredis`](https://github.com/alicebob/miniredis) for in-memory Redis in tests
