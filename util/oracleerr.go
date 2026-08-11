package util

import (
	"errors"

	ora "github.com/sijms/go-ora/v2/network"
)

// oracleNonRetryableCodes 列出由语句本身导致、无法通过重试解决的 Oracle 错误码（ORA-NNNNN）。
// 涵盖语法错误、无效对象/列名、类型转换失败、约束冲突等——这类错误重试无益，直接标记 NonRetryable。
var oracleNonRetryableCodes = map[int]struct{}{
	1:     {}, // 唯一约束冲突（unique constraint violated）
	900:   {}, // invalid SQL statement
	904:   {}, // invalid identifier（无效列名）
	911:   {}, // invalid character
	918:   {}, // column ambiguously defined
	923:   {}, // FROM keyword not found where expected
	933:   {}, // SQL command not properly ended
	936:   {}, // missing expression
	942:   {}, // table or view does not exist
	979:   {}, // not a GROUP BY expression
	1017:  {}, // invalid username/password（鉴权失败，属配置错误）
	1400:  {}, // cannot insert NULL
	1722:  {}, // invalid number
	1843:  {}, // not a valid month
	1858:  {}, // non-numeric character where numeric expected
	2290:  {}, // check constraint violated
	2291:  {}, // integrity constraint（父键不存在）
	2292:  {}, // integrity constraint（子记录存在）
	12899: {}, // value too large for column
}

// WrapOracleError 规范化 Oracle 错误：将由语句本身导致、无法通过重试解决的错误
// （语法错误、无效对象/列名、类型转换失败、约束冲突等）标记为 NonRetryable。
// 其余（如死锁 ORA-00060、资源忙 ORA-00054、网络抖动等）保持可重试。
// 非 Oracle 错误原样返回。
func WrapOracleError(err error) error {
	if err == nil {
		return nil
	}
	var oraErr *ora.OracleError
	if errors.As(err, &oraErr) {
		if _, nonRetryable := oracleNonRetryableCodes[oraErr.ErrCode]; nonRetryable {
			return NonRetryable(err)
		}
	}
	return err
}
