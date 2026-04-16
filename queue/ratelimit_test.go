package queue

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
)

func TestRateLimiterDomainIsolation(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	q := New(mr.Addr())
	t.Cleanup(func() { _ = q.Close() })

	rl := NewRateLimiter(q.Client(), 1.0, 1)

	if err := rl.Wait(context.Background(), "domain-a"); err != nil {
		t.Fatalf("Wait(domain-a) unexpected error: %v", err)
	}
	if err := rl.Wait(context.Background(), "domain-b"); err != nil {
		t.Fatalf("Wait(domain-b) unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	err := rl.Wait(ctx, "domain-a")
	if err == nil {
		t.Fatal("Wait(domain-a) should block and respect context timeout, got nil error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Wait(domain-a) error = %v, want DeadlineExceeded", err)
	}
}

func TestRateLimiterTryConsumeAndRefill(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	q := New(mr.Addr())
	t.Cleanup(func() { _ = q.Close() })

	rl := NewRateLimiter(q.Client(), 50.0, 1)
	key := "ratelmit:test-refill"

	allowed, wait, err := rl.tryConsume(context.Background(), key)
	if err != nil {
		t.Fatalf("first tryConsume() unexpected error: %v", err)
	}
	if !allowed || wait != 0 {
		t.Fatalf("first tryConsume() = (allowed=%v, wait=%v), want (true, 0)", allowed, wait)
	}

	allowed, wait, err = rl.tryConsume(context.Background(), key)
	if err != nil {
		t.Fatalf("second tryConsume() unexpected error: %v", err)
	}
	if allowed {
		t.Fatalf("second tryConsume() allowed=%v, want false", allowed)
	}
	if wait <= 0 {
		t.Fatalf("second tryConsume() wait=%v, want > 0", wait)
	}

	time.Sleep(wait + 15*time.Millisecond)

	allowed, _, err = rl.tryConsume(context.Background(), key)
	if err != nil {
		t.Fatalf("third tryConsume() unexpected error: %v", err)
	}
	if !allowed {
		t.Fatal("third tryConsume() should succeed after refill")
	}
}

func TestRateLimiterRedisFailureIsReturned(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	q := New(mr.Addr())
	rl := NewRateLimiter(q.Client(), 10.0, 1)

	mr.Close()
	t.Cleanup(func() { _ = q.Close() })

	err := rl.Wait(context.Background(), "example.com")
	if err == nil {
		t.Fatal("Wait() against closed Redis returned nil error, want error")
	}
	if !strings.Contains(err.Error(), "rate limit check") {
		t.Fatalf("Wait() error = %q, want wrapped rate-limit context", err.Error())
	}
}
