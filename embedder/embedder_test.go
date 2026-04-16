package embedder

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewDefaults(t *testing.T) {
	t.Parallel()

	e := New("", "", 0)
	if e.baseURL != "http://localhost:11434" {
		t.Fatalf("baseURL = %q, want default %q", e.baseURL, "http://localhost:11434")
	}
	if e.model != DefaultModel {
		t.Fatalf("model = %q, want default %q", e.model, DefaultModel)
	}
	if e.dimension != DefaultDimension {
		t.Fatalf("dimension = %d, want default %d", e.dimension, DefaultDimension)
	}
	if e.batchSize != defaultBatchSize {
		t.Fatalf("batchSize = %d, want default %d", e.batchSize, defaultBatchSize)
	}
	if e.client == nil {
		t.Fatal("client is nil")
	}
	if e.client.Timeout != 120*time.Second {
		t.Fatalf("client timeout = %v, want %v", e.client.Timeout, 120*time.Second)
	}
}

func TestPing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		handler http.HandlerFunc
		wantErr bool
	}{
		{
			name: "200 response succeeds",
			handler: func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/api/tags" {
					t.Fatalf("path = %q, want /api/tags", r.URL.Path)
				}
				w.WriteHeader(http.StatusOK)
			},
			wantErr: false,
		},
		{
			name: "non-200 response returns an error",
			handler: func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(tt.handler)
			defer srv.Close()

			e := New(srv.URL, DefaultModel, DefaultDimension)
			err := e.Ping(context.Background())
			if (err != nil) != tt.wantErr {
				t.Fatalf("Ping() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestPingNetworkError(t *testing.T) {
	t.Parallel()

	e := New("http://127.0.0.1:1", DefaultModel, DefaultDimension)
	err := e.Ping(context.Background())
	if err == nil {
		t.Fatal("Ping() returned nil error for unreachable server, want error")
	}
}

func TestEmbedBatchPrefixesAndBatching(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		batches [][]string
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/embed" {
			t.Fatalf("path = %q, want /api/embed", r.URL.Path)
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/json" {
			t.Fatalf("Content-Type = %q, want application/json", ct)
		}

		var req ollamaEmbedRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}

		mu.Lock()
		batches = append(batches, append([]string(nil), req.Input...))
		mu.Unlock()

		resp := ollamaEmbedResponse{Embeddings: make([][]float32, len(req.Input))}
		for i := range req.Input {
			resp.Embeddings[i] = []float32{float32(i + 1)}
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	e := New(srv.URL, "test-model", DefaultDimension)
	e.batchSize = 2

	texts := []string{"doc one", "doc two", "doc three"}
	got, err := e.EmbedBatch(context.Background(), texts)
	if err != nil {
		t.Fatalf("EmbedBatch() unexpected error: %v", err)
	}
	if len(got) != len(texts) {
		t.Fatalf("EmbedBatch() returned %d embeddings, want %d", len(got), len(texts))
	}

	mu.Lock()
	defer mu.Unlock()
	if len(batches) != 2 {
		t.Fatalf("number of batches = %d, want 2", len(batches))
	}
	for _, batch := range batches {
		for _, input := range batch {
			if !strings.HasPrefix(input, InstructDocument) {
				t.Fatalf("batch input %q missing document instruction prefix", input)
			}
		}
	}
}

func TestEmbedQueryUsesQueryPrefix(t *testing.T) {
	t.Parallel()

	var capturedInput string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req ollamaEmbedRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		if len(req.Input) != 1 {
			t.Fatalf("input length = %d, want 1", len(req.Input))
		}
		capturedInput = req.Input[0]
		_ = json.NewEncoder(w).Encode(ollamaEmbedResponse{Embeddings: [][]float32{{0.42}}})
	}))
	defer srv.Close()

	e := New(srv.URL, "test-model", DefaultDimension)
	vec, err := e.EmbedQuery(context.Background(), "where is sherlock?")
	if err != nil {
		t.Fatalf("EmbedQuery() unexpected error: %v", err)
	}
	if len(vec) != 1 || vec[0] != 0.42 {
		t.Fatalf("EmbedQuery() = %v, want [0.42]", vec)
	}
	if !strings.HasPrefix(capturedInput, InstructQuery) {
		t.Fatalf("captured input = %q, want query instruction prefix", capturedInput)
	}
}

func TestEmbedBatchReturnsErrorOnMismatchedEmbeddingsCount(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(ollamaEmbedResponse{Embeddings: [][]float32{{1.0}}})
	}))
	defer srv.Close()

	e := New(srv.URL, "test-model", DefaultDimension)
	_, err := e.EmbedBatch(context.Background(), []string{"a", "b"})
	if err == nil {
		t.Fatal("EmbedBatch() with mismatched response count returned nil error, want error")
	}
	if !strings.Contains(err.Error(), "expected 2 embeddings") {
		t.Fatalf("error = %q, want mismatch count context", err.Error())
	}
}

func TestEmbedBatchHTTPErrorIncludesResponseBody(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = fmt.Fprint(w, "bad request payload")
	}))
	defer srv.Close()

	e := New(srv.URL, "test-model", DefaultDimension)
	_, err := e.EmbedBatch(context.Background(), []string{"a"})
	if err == nil {
		t.Fatal("EmbedBatch() expected error for non-200 response")
	}
	if !strings.Contains(err.Error(), "bad request payload") {
		t.Fatalf("error = %q, want server response body", err.Error())
	}
}
