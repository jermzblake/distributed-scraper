package chunker

import (
	"strings"
	"unicode"
)

// Chunk represents one text segment with its position metadata.
type Chunk struct {
	Text string
	Index int // chunk number within its parent document
	WordCount int
}

const (
	DefaultChunkSize = 400 	// target words per chunk (Technical documentation benefits from larger chunk. FAQ-style content benefits from smaller chunks)
	DefaultChunkOverlap = 50 	// words shared between adjacent chunks
)

// Split divides text into overlapping word-based chunks.
//
// Overlap is critical for retrieval: if a sentence about "refund policy"
// straddles two chunks, overlap ensures at least one chunk contains the
// complete sentence, so a search for "refund policy" will match it.
func Split(text string, chunkSize, chunkOverlap int) []Chunk {
	if chunkSize <= 0 {
		chunkSize = DefaultChunkSize
	}
	if chunkOverlap < 0 {
		chunkOverlap = DefaultChunkOverlap
	}
	if chunkOverlap >= chunkSize {
		chunkOverlap = chunkSize - 1
	}

	step := chunkSize - chunkOverlap
	if step <= 0 {
		step = 1
	}

	words := tokenize(text)
	if len(words) == 0 {
		return nil
	}

	var chunks []Chunk
	idx := 0

	for start := 0; start < len(words); start += step {
		end := start + chunkSize
		if end > len(words) {
			end = len(words)
		}

		segment := words[start:end]
		text := strings.Join(segment, " ")

		chunks = append(chunks, Chunk{
			Text:      text,
			Index:     idx,
			WordCount: len(segment),
		})
		idx++

		// Don't create a tiny orphan chunk at the end.
		// If the remaining words after this chunk are fewer than
		// the overlap, they're already covered by this chunk.
		if end == len(words) {
			break
		}
	}

	return chunks
}

// tokenize splits text into words, collapsing all whitespace.
// Keeping punctuation attached to words (e.g., "policy.") is intentional —
// splitting on punctuation would fragment sentences.
func tokenize(text string) []string {
	var words []string
	var current strings.Builder

	for _, r := range text {
		if unicode.IsSpace(r) {
			if current.Len() > 0 {
				words = append(words, current.String())
				current.Reset()
			}
		} else {
			current.WriteRune(r)
		}
	}

	if current.Len() > 0 {
		words = append(words, current.String())
	}

	return words
}