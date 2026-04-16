package main

import (
	"context"
	"fmt"
	"log"

	"distributed-scraper/queue"
)

func main() {
	q := queue.New("localhost:6379")
	ctx := context.Background()

	// A small crawlable site won't ban you
	seeds := []string{
		"https://books.toscrape.com/",
		"https://quotes.toscrape.com/",
	}

	for _, url := range seeds {
		if err := q.Push(ctx, url); err != nil {
			log.Fatalf("Failed to push seed URL %s: %v", url, err)
		} else {
			fmt.Printf("Seed URL pushed: %s", url)
		}
	}
}