package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	"distributed-scraper/embedder"
	"distributed-scraper/vectorstore"

	pb "github.com/qdrant/go-client/qdrant"
)

const defaultQuery = "mystery books with good reviews"

type queryEmbedder interface {
	EmbedQuery(ctx context.Context, query string) ([]float32, error)
}

type searchStore interface {
	Search(ctx context.Context, vector []float32, limit uint64) ([]*pb.ScoredPoint, error)
}

func main() {
	err := execute(
		context.Background(),
		os.Args[1:],
		os.Stdout,
		func() queryEmbedder {
			return embedder.New(os.Getenv("OLLAMA_ADDR"), embedder.DefaultModel, embedder.DefaultDimension)
		},
		func() (searchStore, error) {
			return vectorstore.New("localhost:6334", false)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

func execute(
	ctx context.Context,
	args []string,
	out io.Writer,
	newEmbedder func() queryEmbedder,
	newStore func() (searchStore, error),
) error {
	store, err := newStore()
	if err != nil {
		return err
	}
	return run(ctx, args, out, newEmbedder(), store)
}

func run(ctx context.Context, args []string, out io.Writer, emb queryEmbedder, store searchStore) error {
	query := defaultQuery
	if len(args) > 0 {
		query = args[0]
	}

	queryVector, err := emb.EmbedQuery(ctx, query)
	if err != nil {
		return err
	}

	results, err := store.Search(ctx, queryVector, 5)
	if err != nil {
		return err
	}

	renderResults(out, query, results)
	return nil
}

func renderResults(out io.Writer, query string, results []*pb.ScoredPoint) {
	fmt.Fprintf(out, "Top results for: %q\n\n", query)
	for i, r := range results {
		fmt.Fprintf(out, "%d. [score: %.3f]\n", i+1, r.Score)
		fmt.Fprintf(out, "   URL: %s\n", payloadString(r, "url"))
		fmt.Fprintf(out, "   Title: %s\n", payloadString(r, "title"))
		fmt.Fprintf(out, "   Text: %.120s...\n\n", payloadString(r, "text"))
	}
}


func payloadString(r *pb.ScoredPoint, key string) string {
	if r == nil || r.Payload == nil {
		return ""
	}
	v, ok := r.Payload[key]
	if !ok || v == nil {
		return ""
	}
	return v.GetStringValue()
}
