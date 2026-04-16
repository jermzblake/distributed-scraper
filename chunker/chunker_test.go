package chunker

import (
	"strings"
	"testing"
)

func makeWords(n int) string {
	parts := make([]string, n)
	for i := 0; i < n; i++ {
		parts[i] = "w"
	}
	return strings.Join(parts, " ")
}

func TestSplitEmptyInput(t *testing.T) {
	t.Parallel()

	got := Split("   \n\t  ", 10, 2)
	if got != nil {
		t.Fatalf("Split(empty) = %v, want nil", got)
	}
}

func TestSplitSingleChunk(t *testing.T) {
	t.Parallel()

	got := Split("alpha beta gamma", 10, 2)
	if len(got) != 1 {
		t.Fatalf("len(chunks) = %d, want 1", len(got))
	}
	if got[0].Index != 0 {
		t.Fatalf("chunk[0].Index = %d, want 0", got[0].Index)
	}
	if got[0].WordCount != 3 {
		t.Fatalf("chunk[0].WordCount = %d, want 3", got[0].WordCount)
	}
	if got[0].Text != "alpha beta gamma" {
		t.Fatalf("chunk[0].Text = %q, want %q", got[0].Text, "alpha beta gamma")
	}
}

func TestSplitOverlapAndIndexMetadata(t *testing.T) {
	t.Parallel()

	text := "one two three four five six seven"
	got := Split(text, 4, 2)

	if len(got) != 3 {
		t.Fatalf("len(chunks) = %d, want 3", len(got))
	}

	wantTexts := []string{
		"one two three four",
		"three four five six",
		"five six seven",
	}
	wantCounts := []int{4, 4, 3}
	for i := range got {
		if got[i].Index != i {
			t.Fatalf("chunk[%d].Index = %d, want %d", i, got[i].Index, i)
		}
		if got[i].Text != wantTexts[i] {
			t.Fatalf("chunk[%d].Text = %q, want %q", i, got[i].Text, wantTexts[i])
		}
		if got[i].WordCount != wantCounts[i] {
			t.Fatalf("chunk[%d].WordCount = %d, want %d", i, got[i].WordCount, wantCounts[i])
		}
	}
}

func TestSplitInvalidOverlapDoesNotStall(t *testing.T) {
	t.Parallel()

	// overlap > chunkSize used to risk a non-advancing loop.
	text := makeWords(8)
	got := Split(text, 3, 10)
	if len(got) == 0 {
		t.Fatal("Split() returned zero chunks, want at least one")
	}
	if len(got) > 8 {
		t.Fatalf("len(chunks) = %d, want <= 8", len(got))
	}
	for i, c := range got {
		if c.Index != i {
			t.Fatalf("chunk[%d].Index = %d, want %d", i, c.Index, i)
		}
	}
}

func TestSplitNegativeOverlapUsesDefaultSafely(t *testing.T) {
	t.Parallel()

	got := Split(makeWords(6), 3, -1)
	if len(got) == 0 {
		t.Fatal("Split() returned no chunks")
	}
	if got[0].WordCount == 0 {
		t.Fatal("first chunk has zero words")
	}
}

func TestSplitUsesDefaultChunkSize(t *testing.T) {
	t.Parallel()

	text := makeWords(DefaultChunkSize + 5)
	got := Split(text, 0, 0)
	if len(got) != 2 {
		t.Fatalf("len(chunks) = %d, want 2", len(got))
	}
	if got[0].WordCount != DefaultChunkSize {
		t.Fatalf("first chunk words = %d, want %d", got[0].WordCount, DefaultChunkSize)
	}
	if got[1].WordCount != 5 {
		t.Fatalf("second chunk words = %d, want 5", got[1].WordCount)
	}
}

func TestTokenizeCollapsesWhitespaceAndKeepsPunctuation(t *testing.T) {
	t.Parallel()

	got := tokenize("  Hello,\n\tworld.  spaced\u00A0text  ")
	want := []string{"Hello,", "world.", "spaced", "text"}

	if len(got) != len(want) {
		t.Fatalf("len(tokens) = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("token[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}
