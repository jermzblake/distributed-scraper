package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"distributed-scraper/embedder"
	"distributed-scraper/vectorstore"
)

func main() {
	query := "mystery books with good reviews"
	if len(os.Args) > 1 {
		query = os.Args[1]
	}

	emb := embedder.New(os.Getenv("OLLAMA_ADDR"), embedder.DefaultModel, embedder.DefaultDimension)
	store, err := vectorstore.New("localhost:6334", false)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	queryVector, err := emb.EmbedQuery(ctx, query)
	if err != nil {
		log.Fatal(err)
	}

	results, err := store.Search(ctx, queryVector, 5)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("Top results for: %q\n\n", query)
	for i, r := range results {
		fmt.Printf("%d. [score: %.3f]\n", i+1, r.Score)
		fmt.Printf("   URL: %s\n", r.Payload["url"].GetStringValue())
		fmt.Printf("   Title: %s\n", r.Payload["title"].GetStringValue())
		fmt.Printf("   Text: %.120s...\n\n", r.Payload["text"].GetStringValue())
	}
}
