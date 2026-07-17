package util

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

// WrapPgError 规范化 PostgreSQL 错误：补充 DETAIL/CONTEXT 诊断信息，
// 并对无法通过重试解决的错误类别（数据异常、完整性约束、语法错误）标记为 NonRetryable。
// 非 PG 错误原样返回。
func WrapPgError(err error) error {
	if err == nil {
		return nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && len(pgErr.Code) >= 2 {
		// 补充 DETAIL 与 CONTEXT 字段：它们不包含在 PgError.Error() 中，
		// 却承载了最关键的定位信息（例如 COPY 期间是哪一列、哪一行触发的错误）。
		enriched := pgEnrich(err, pgErr)
		switch pgErr.Code[:2] {
		case "22", // Data Exception (type mismatches, overflow, …)
			"23", // Integrity Constraint Violation (not-null, FK, unique, check)
			"42": // Syntax Error or Access Rule Violation (bad column, bad table, …)
			return NonRetryable(enriched)
		}
		return enriched
	}
	return err
}

// IsPgLockTimeout 判断是否为 PostgreSQL lock_timeout 触发的错误（错误码 55P03）。
func IsPgLockTimeout(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "55P03"
}

// pgEnrich 用 pgErr 的 DETAIL 与 CONTEXT 字段包装 err，
// 使打印错误的调用方能看到完整诊断上下文，而不仅仅是简短的错误消息。
// 使用 %w 保留错误链，便于 errors.Is / errors.As。
func pgEnrich(err error, pgErr *pgconn.PgError) error {
	if pgErr.Detail == "" && pgErr.Where == "" {
		return err
	}
	extra := ""
	if pgErr.Detail != "" {
		extra += "; DETAIL: " + pgErr.Detail
	}
	if pgErr.Where != "" {
		extra += "; CONTEXT: " + pgErr.Where
	}
	return fmt.Errorf("%w%s", err, extra)
}
