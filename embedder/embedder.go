package embedder

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// *NOTE*: Switch to float32 if memory/throughput is the bottleneck, and benchmark retrieval quality (recall@k / ranking changes) before committing.

// Model constants — swap by changing DefaultModel and DefaultDimension together.
const (
	ModelQwen3_0_6B = "qwen3-embedding:0.6b" // CPU-friendly, 1024 dims
	ModelQwen3_4B   = "qwen3-embedding:4b"   // ~3GB VRAM, 2560 dims
	ModelQwen3_8B   = "qwen3-embedding:8b"   // ~5GB VRAM, 4096 dims

	DefaultModel     = ModelQwen3_0_6B
	DefaultDimension = uint64(1024)

	// Ollama batches well but embedding large batches blocks the server.
	// 64 is a safe ceiling for the 0.6B model on CPU.
	// Increase to 256 if you're running the 4B/8B on a GPU.
	defaultBatchSize = 64

	// qwen3-embedding supports task instructions that meaningfully improve
	// retrieval quality. These are the recommended prefixes per Qwen3 docs.
	// Including the instruction in every chunk text improves cosine similarity
	// for asymmetric retrieval (short query vs long document).
	InstructDocument = "Represent this document for retrieval: "
	InstructQuery    = "Represent this query for retrieving relevant documents: "
)

// Embedder calls the local Ollama /api/embed endpoint.
type Embedder struct {
	baseURL   string
	model     string
	dimension uint64
	batchSize int
	client    *http.Client
}

// New creates an Embedder pointing at the local Ollama server.
func New(addr, model string, dimension uint64) *Embedder {
	if addr == "" {
		addr = "http://localhost:11434"
	}
	if model == "" {
		model = DefaultModel
	}
	if dimension == 0 {
		dimension = DefaultDimension
	}

	return &Embedder{
		baseURL:   addr,
		model:     model,
		dimension: dimension,
		batchSize: defaultBatchSize,
		client: &http.Client{
			// Embedding a large batch on CPU can take 10-30s.
			// Don't use the default no-timeout client here.
			Timeout: 120 * time.Second,
		},
	}
}

// Ping verifies the Ollama server is reachable. Call at worker startup.
func (e *Embedder) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, e.baseURL+"/api/tags", nil)
	if err != nil {
		return err
	}
	resp, err := e.client.Do(req)
	if err != nil {
		return fmt.Errorf("ollama not reachable at %s: %w", e.baseURL, err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ollama ping returned %d", resp.StatusCode)
	}
	return nil
}

// EmbedBatch embeds a slice of strings in one or more batched API calls.
// The returned slice is always the same length as the input — embeddings[i]
// corresponds to texts[i].
//
// Each text will be prefixed with InstructDocument automatically.
// If you are embedding a search query instead of a document, use EmbedQuery.
func (e *Embedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	return e.embedWithInstruct(ctx, texts, InstructDocument)
}

// EmbedQuery embeds a single search query with the query instruction prefix.
// Use this in your search CLI, not during ingestion.
func (e *Embedder) EmbedQuery(ctx context.Context, query string) ([]float32, error) {
	results, err := e.embedWithInstruct(ctx, []string{query}, InstructQuery)
	if err != nil {
		return nil, err
	}
	return results[0], nil
}

func (e *Embedder) embedWithInstruct(ctx context.Context, texts []string, instruct string) ([][]float32, error) {
	if len(texts) == 0 {
		return nil, nil
	}

	// Prepend instruction prefix to every text.
	instructed := make([]string, len(texts))
	for i, t := range texts {
		instructed[i] = instruct + t
	}

	var allEmbeddings [][]float32
	for start := 0; start < len(instructed); start += e.batchSize {
		end := start + e.batchSize
		if end > len(instructed) {
			end = len(instructed)
		}
		
		batch, err := e.callOllama(ctx, instructed[start:end])
		if err != nil {
			return nil, fmt.Errorf("batch starting at %d: %w", start, err)
		}
		allEmbeddings = append(allEmbeddings, batch...)
	}

	return allEmbeddings, nil
}

// ollamaEmbedRequest is the JSON body for POST /api/embed.
type ollamaEmbedRequest struct {
	Model    string   `json:"model"`
	Input    []string `json:"input"`
	Truncate bool     `json:"truncate"` // truncate inputs that exceed context length
}

// ollamaEmbedResponse is the JSON response from POST /api/embed.
type ollamaEmbedResponse struct {
	Embeddings [][]float32 `json:"embeddings"`
	// Ollama also returns "model", "total_duration", etc. — we ignore them.
}

func (e *Embedder) callOllama(ctx context.Context, texts []string) ([][]float32, error) {
	body, err := json.Marshal(ollamaEmbedRequest{
		Model:    e.model,
		Input:    texts,
		Truncate: true,	// safe default: truncate rather than error on long inputs
	})
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.baseURL+"/api/embed", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := e.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("Ollama request HTTP error: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("Ollama request returned %d: %s", resp.StatusCode, string(b))
	}

	var result ollamaEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode Ollama response: %w", err)
	}

	if len(result.Embeddings) != len(texts) {
		return nil, fmt.Errorf("expected %d embeddings, got %d", len(texts), len(result.Embeddings))
	}

	return result.Embeddings, nil
}

// Dimension returns the vector size this model produces.
// Qdrant needs this to create a collection with the right index size.
func (e *Embedder) Dimension() uint64 {
	return e.dimension
}