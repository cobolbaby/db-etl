package reader

import (
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type OracleReader struct {
	*BaseReader
}

type oracleDialect struct{}

func NewOracleReader(db *sql.DB, src *config.SourceConfig) Reader {
	return &OracleReader{
		BaseReader: &BaseReader{
			conn:    db,
			Source:  src,
			dialect: oracleDialect{},
		},
	}
}

func (oracleDialect) buildBaseQuery(source *config.SourceConfig, projection string, whereClause string) (string, error) {
	var query string

	if source.SQL != "" {
		// Oracle 支持内联子查询，别名不能带 AS，直接用 t。
		query = fmt.Sprintf("SELECT %s FROM (%s) t WHERE %s", projection, source.SQL, whereClause)
	} else if source.Table != "" {
		// Oracle 无 MSSQL 的 NOLOCK 提示，默认多版本读一致性，普通 SELECT 不阻塞写入。
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s", projection, source.Table, whereClause)
	} else {
		return "", fmt.Errorf("source sql or table is required")
	}

	return query, nil
}

// quoteIdentifier 用双引号包裹标识符。
// 注意：Oracle 未加引号的标识符会被折叠为大写，一旦加引号则区分大小写；
// 因此 fields_mapping 中的列名/别名应与数据库实际大小写一致（通常为大写）。
func (oracleDialect) quoteIdentifier(identifier string) string {
	if isAlreadyQuotedIdentifier(identifier) {
		return identifier
	}
	return `"` + identifier + `"`
}

func (oracleDialect) wrapError(err error) error {
	return util.WrapOracleError(err)
}

func (oracleDialect) getColumnHandler(dbType string) ColHandler {
	upper := strings.ToUpper(dbType)

	// 二进制类型（RAW / LongRaw / BLOB）：go-ora 以 []byte 返回，
	// 默认 LOB 内联模式下 BLOB 列的类型名会呈现为 LONGRAW。
	// 统一转成 PostgreSQL bytea 在 COPY CSV 中识别的 \x 十六进制格式。
	if strings.Contains(upper, "RAW") || strings.Contains(upper, "BLOB") {
		return func(v any) string {
			if v == nil {
				return util.NullSentinel
			}
			if b, ok := v.([]byte); ok {
				if len(b) == 0 {
					return `\x`
				}
				return `\x` + hex.EncodeToString(b)
			}
			return defaultColumnHandler(v)
		}
	}

	// 日期/时间类型：go-ora 以 time.Time 返回。
	// DATE、TIMESTAMP、TIMESTAMP WITH [LOCAL] TIME ZONE 等在类型名中均含 DATE/TIMESTAMP/TIMETZ。
	// 保留至纳秒的小数秒精度（尾随零自动去除），覆盖 Oracle TIMESTAMP(9)。
	if upper == "DATE" || strings.Contains(upper, "TIMESTAMP") || strings.Contains(upper, "TIMETZ") {
		return func(v any) string {
			if v == nil {
				return util.NullSentinel
			}
			if t, ok := v.(time.Time); ok {
				return t.Format("2006-01-02 15:04:05.999999999")
			}
			return defaultColumnHandler(v)
		}
	}

	// NUMBER 由 go-ora 以字符串返回（完整精度，无 float64 精度损失）；
	// VARCHAR2/CHAR/NCHAR/CLOB/LONG 等亦为字符串，统一交由默认处理器。
	return defaultColumnHandler
}
