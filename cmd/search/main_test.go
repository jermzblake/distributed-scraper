package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	pb "github.com/qdrant/go-client/qdrant"
)

type fakeEmbedder struct {
	vector   []float32
	err      error
	gotQuery string
}

func (f *fakeEmbedder) EmbedQuery(_ context.Context, query string) ([]float32, error) {
	f.gotQuery = query
	if f.err != nil {
		return nil, f.err
	}
	return f.vector, nil
}

type fakeStore struct {
	results   []*pb.ScoredPoint
	err       error
	gotVector []float32
	gotLimit  uint64
}

func (f *fakeStore) Search(_ context.Context, vector []float32, limit uint64) ([]*pb.ScoredPoint, error) {
	f.gotVector = append([]float32(nil), vector...)
	f.gotLimit = limit
	if f.err != nil {
		return nil, f.err
	}
	return f.results, nil
}

func TestRunUsesDefaultQueryAndFixedLimit(t *testing.T) {
	t.Parallel()

	emb := &fakeEmbedder{vector: []float32{0.1, 0.2}}
	store := &fakeStore{}
	var out bytes.Buffer

	err := run(context.Background(), nil, &out, emb, store)
	if err != nil {
		t.Fatalf("run() unexpected error: %v", err)
	}
	if emb.gotQuery != defaultQuery {
		t.Fatalf("query = %q, want default %q", emb.gotQuery, defaultQuery)
	}
	if store.gotLimit != 5 {
		t.Fatalf("limit = %d, want 5", store.gotLimit)
	}
	if len(store.gotVector) != 2 || store.gotVector[0] != 0.1 || store.gotVector[1] != 0.2 {
		t.Fatalf("vector = %v, want [0.1 0.2]", store.gotVector)
	}
	if !strings.Contains(out.String(), `Top results for: "`+defaultQuery+`"`) {
		t.Fatalf("output missing header for default query: %q", out.String())
	}
}

func TestRunUsesFirstArgQuery(t *testing.T) {
	t.Parallel()

	emb := &fakeEmbedder{vector: []float32{1.0}}
	store := &fakeStore{}
	var out bytes.Buffer

	err := run(context.Background(), []string{"custom query", "ignored"}, &out, emb, store)
	if err != nil {
		t.Fatalf("run() unexpected error: %v", err)
	}
	if emb.gotQuery != "custom query" {
		t.Fatalf("query = %q, want %q", emb.gotQuery, "custom query")
	}
	if !strings.Contains(out.String(), `Top results for: "custom query"`) {
		t.Fatalf("output missing header for provided query: %q", out.String())
	}
}

func TestExecuteReturnsStoreCreationError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("store init failed")
	err := execute(
		context.Background(),
		nil,
		&bytes.Buffer{},
		func() queryEmbedder { return &fakeEmbedder{} },
		func() (searchStore, error) { return nil, wantErr },
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("execute() error = %v, want %v", err, wantErr)
	}
}

func TestRunReturnsEmbedError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("embed failed")
	emb := &fakeEmbedder{err: wantErr}
	store := &fakeStore{}

	err := run(context.Background(), nil, &bytes.Buffer{}, emb, store)
	if !errors.Is(err, wantErr) {
		t.Fatalf("run() error = %v, want %v", err, wantErr)
	}
}

func TestRunReturnsSearchError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("search failed")
	emb := &fakeEmbedder{vector: []float32{0.3}}
	store := &fakeStore{err: wantErr}

	err := run(context.Background(), nil, &bytes.Buffer{}, emb, store)
	if !errors.Is(err, wantErr) {
		t.Fatalf("run() error = %v, want %v", err, wantErr)
	}
}

func TestRunRendersResultsAndHandlesMissingPayloadKeys(t *testing.T) {
	t.Parallel()

	longText := strings.Repeat("a", 140)
	results := []*pb.ScoredPoint{
		{
			Score: 0.91,
			Payload: map[string]*pb.Value{
				"url":   {Kind: &pb.Value_StringValue{StringValue: "https://example.com"}},
				"title": {Kind: &pb.Value_StringValue{StringValue: "Example"}},
				"text":  {Kind: &pb.Value_StringValue{StringValue: longText}},
			},
		},
		{
			Score:   0.50,
			Payload: map[string]*pb.Value{},
		},
	}

	emb := &fakeEmbedder{vector: []float32{1}}
	store := &fakeStore{results: results}
	var out bytes.Buffer

	err := run(context.Background(), []string{"q"}, &out, emb, store)
	if err != nil {
		t.Fatalf("run() unexpected error: %v", err)
	}

	s := out.String()
	if !strings.Contains(s, "1. [score: 0.910]") {
		t.Fatalf("output missing first score line: %q", s)
	}
	if !strings.Contains(s, "2. [score: 0.500]") {
		t.Fatalf("output missing second score line: %q", s)
	}
	if !strings.Contains(s, "URL: https://example.com") {
		t.Fatalf("output missing URL field: %q", s)
	}
	if !strings.Contains(s, "Title: Example") {
		t.Fatalf("output missing title field: %q", s)
	}
	if !strings.Contains(s, "Text: ") {
		t.Fatalf("output missing text field: %q", s)
	}
}
