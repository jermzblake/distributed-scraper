package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

/** RateLimiter implements a per-domain token bucket in Redis.
*
* How a token bucket works:
* - The bucket holds up to `burst` tokens.
* - Tokens refill at `rate` tokens per second.
* - Each request consumes one token.
* - If the bucket is empty, the caller must wait.
*
* Storing the bucket in Redis means ALL workers share the same
* rate limit per domain — exactly what you want in a distributed system.
*
*/
type RateLimiter struct {
	client *redis.Client
	rate  float64 // tokens per second
	burst int     // max tokens in bucket
}

func NewRateLimiter(client *redis.Client, ratePerSecond float64, burst int) *RateLimiter {
	return &RateLimiter{
		client: client,
		rate:   ratePerSecond,
		burst:  burst,
	}
}

// Wait blocks until a token is available for the given domain key.
// *This is a simplified token bucket — for production use consider
// *the sliding window log pattern or a dedicated library.
func (rl *RateLimiter) Wait(ctx context.Context, domain string) error {
	key := "ratelmit:" + domain

	for {
		allowed, retryAfter, err := rl.tryConsume(ctx, key)
		if err != nil {
			return fmt.Errorf("rate limit check: %w", err)
		}
		if allowed {
			return nil // Token consumed, proceed with request
		}

		// No token available, wait for retryAfter duration before retrying
		select {
		case <-time.After(retryAfter):
			continue // Retry after waiting
		case <-ctx.Done():
			return ctx.Err() // Context cancelled or timed out
		}
	}
}

// tryConsume is implemented with a Lua script to make the
// check-and-decrement atomic.
var tokenBucketScript = redis.NewScript(`
local key = KEYS[1]
local rate = tonumber(ARGV[1])      -- tokens per second
local burst = tonumber(ARGV[2])     -- max tokens
local now = tonumber(ARGV[3])       -- current unix timestamp (ms)

local data = redis.call("HMGET", key, "tokens", "last_refill")
local tokens = tonumber(data[1]) or burst
local last_refill = tonumber(data[2]) or now

-- Refill tokens based on time elapsed
local elapsed = (now - last_refill) / 1000  -- convert ms to seconds
local new_tokens = math.min(burst, tokens + elapsed * rate)

if new_tokens >= 1 then
    -- Consume one token
    redis.call("HMSET", key, "tokens", new_tokens - 1, "last_refill", now)
    redis.call("PEXPIRE", key, 60000)  -- expire after 60s of inactivity
    return {1, 0}  -- allowed, no wait
else
    -- Calculate how long until next token
    local wait_ms = math.ceil((1 - new_tokens) / rate * 1000)
    return {0, wait_ms}  -- not allowed, wait this many ms
end
`)

func (rl *RateLimiter) tryConsume(ctx context.Context, key string) (bool, time.Duration, error) {
	nowMs := time.Now().UnixMilli()

	result, err := tokenBucketScript.Run(ctx, rl.client, []string{key},
		rl.rate, rl.burst, nowMs).Int64Slice()
		if err != nil {
			return false, 0, err
		}

	allowed := result[0] == 1
	waitMs := time.Duration(result[1]) * time.Millisecond

	return allowed, waitMs, nil
}