package util

import (
	"errors"
	"fmt"
	"log"
	"math"
	"time"
)

// NonRetryableError wraps an error that should not be retried.
type NonRetryableError struct {
	Err error
}

func (e *NonRetryableError) Error() string { return e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

// NonRetryable wraps err to signal that Retry should abort immediately without further attempts.
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &NonRetryableError{Err: err}
}

// RetryConfig holds retry behavior settings.
type RetryConfig struct {
	MaxAttempts int           // Maximum number of attempts (including the first). 0 or 1 means no retry.
	Delay       time.Duration // Initial delay between retries.
	MaxDelay    time.Duration // Maximum delay (caps exponential backoff).
	Multiplier  float64       // Backoff multiplier per attempt. 0 means use 2.0.
}

// DefaultRetryConfig returns a sensible default retry config.
func DefaultRetryConfig() RetryConfig {
	return RetryConfig{
		MaxAttempts: 3,
		Delay:       5 * time.Second,
		MaxDelay:    60 * time.Second,
		Multiplier:  2.0,
	}
}

// Retry executes fn up to cfg.MaxAttempts times. If fn returns nil, Retry returns nil immediately.
// On failure, it waits with exponential backoff before retrying.
func Retry(label string, cfg RetryConfig, fn func() error) error {
	if cfg.MaxAttempts <= 1 {
		return fn()
	}
	if cfg.Multiplier <= 0 {
		cfg.Multiplier = 2.0
	}
	if cfg.MaxDelay <= 0 {
		cfg.MaxDelay = 60 * time.Second
	}

	var lastErr error
	for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}

		var nre *NonRetryableError
		if errors.As(lastErr, &nre) { // 不可重试，立即返回
			return nre.Err
		}

		if attempt == cfg.MaxAttempts { // 已到上限，跳出
			break
		}

		delay := time.Duration(float64(cfg.Delay) * math.Pow(cfg.Multiplier, float64(attempt-1)))
		if delay > cfg.MaxDelay {
			delay = cfg.MaxDelay
		}
		log.Printf("[retry] %s attempt %d/%d failed: %v, retrying in %s",
			label, attempt, cfg.MaxAttempts, lastErr, delay)
		time.Sleep(delay)
	}

	return fmt.Errorf("%s failed after %d attempts: %w", label, cfg.MaxAttempts, lastErr)
}
