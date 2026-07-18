package util

import (
	"errors"
	"fmt"
	"testing"

	mssql "github.com/microsoft/go-mssqldb"
)

func TestWrapMSSQLErrorMarksSyntaxErrorNonRetryable(t *testing.T) {
	// Class 15 = 语法错误（如 Incorrect syntax near the keyword 'with'）。
	err := mssql.Error{Number: 156, Class: 15, Message: "Incorrect syntax near the keyword 'with'."}
	wrapped := WrapMSSQLError(fmt.Errorf("get column handlers: %w", err))

	var nre *NonRetryableError
	if !errors.As(wrapped, &nre) {
		t.Fatalf("expected syntax error to be NonRetryable, got %v", wrapped)
	}
}

func TestWrapMSSQLErrorKeepsDeadlockRetryable(t *testing.T) {
	// 死锁 1205 严重级别为 13，落在 11-16 区间，但属瞬时错误，应保持可重试。
	err := mssql.Error{Number: 1205, Class: 13, Message: "deadlock victim"}
	wrapped := WrapMSSQLError(err)

	var nre *NonRetryableError
	if errors.As(wrapped, &nre) {
		t.Fatalf("expected deadlock to stay retryable, got NonRetryable")
	}
}

func TestWrapMSSQLErrorPassesThroughNonMSSQL(t *testing.T) {
	err := errors.New("plain error")
	if got := WrapMSSQLError(err); got != err {
		t.Fatalf("expected non-mssql error to pass through unchanged, got %v", got)
	}
}
