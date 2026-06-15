package util

import (
	"errors"
	"testing"
	"time"
)

func TestRetry_SuccessOnFirstAttempt(t *testing.T) {
	calls := 0
	err := Retry("test", RetryConfig{MaxAttempts: 3, Delay: time.Millisecond}, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}

func TestRetry_SuccessOnThirdAttempt(t *testing.T) {
	calls := 0
	err := Retry("test", RetryConfig{MaxAttempts: 3, Delay: time.Millisecond, MaxDelay: 10 * time.Millisecond}, func() error {
		calls++
		if calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestRetry_AllFail(t *testing.T) {
	calls := 0
	err := Retry("test", RetryConfig{MaxAttempts: 2, Delay: time.Millisecond, MaxDelay: 10 * time.Millisecond}, func() error {
		calls++
		return errors.New("permanent")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 2 {
		t.Fatalf("expected 2 calls, got %d", calls)
	}
}

func TestRetry_NoRetry(t *testing.T) {
	calls := 0
	err := Retry("test", RetryConfig{MaxAttempts: 1}, func() error {
		calls++
		return errors.New("fail")
	})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("expected 1 call, got %d", calls)
	}
}
