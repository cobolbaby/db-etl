package util

import (
	"errors"

	mssql "github.com/microsoft/go-mssqldb"
)

// mssqlTransientErrorNumbers 列出严重级别虽落在 11-16 区间、但仍可能通过重试成功的 MSSQL 错误号。
// 这些属于并发/锁竞争类瞬时错误，不应被标记为不可重试。
var mssqlTransientErrorNumbers = map[int32]struct{}{
	1204: {}, // 无法获得锁资源
	1205: {}, // 事务被选为死锁牺牲者
	1222: {}, // 锁请求超时
}

// WrapMSSQLError 规范化 SQL Server 错误：将由语句本身导致、无法通过重试解决的错误
// （语法错误、无效列名/对象名、类型转换失败等）标记为 NonRetryable。
//
// 判定依据为错误的严重级别（Class/severity）：SQL Server 中 11-16 表示“可由用户纠正”的错误
// （例如 15=语法错误，16=一般性语句错误、无效对象名），这类错误重试无益。
// 但其中的并发/锁类瞬时错误（死锁、锁超时）例外，仍保持可重试。
// 非 MSSQL 错误原样返回。
func WrapMSSQLError(err error) error {
	if err == nil {
		return nil
	}
	var mssqlErr mssql.Error
	if errors.As(err, &mssqlErr) {
		if _, transient := mssqlTransientErrorNumbers[mssqlErr.Number]; transient {
			return err
		}
		if mssqlErr.Class >= 11 && mssqlErr.Class <= 16 {
			return NonRetryable(err)
		}
	}
	return err
}
