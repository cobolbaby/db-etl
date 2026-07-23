package reader

import (
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"strings"
	"time"
)

type PGReader struct {
	*BaseReader
}

type pgDialect struct{}

func NewPGReader(db *sql.DB, src *config.SourceConfig) Reader {
	return &PGReader{
		BaseReader: &BaseReader{
			DB:      db,
			Source:  src,
			dialect: pgDialect{},
		},
	}
}

func (pgDialect) buildBaseQuery(source *config.SourceConfig, projection string, whereClause string) (string, error) {
	var query string

	if source.SQL != "" {
		query = fmt.Sprintf("SELECT %s FROM (%s) t WHERE %s", projection, source.SQL, whereClause)
	} else if source.Table != "" {
		query = fmt.Sprintf("SELECT %s FROM %s WHERE %s", projection, source.Table, whereClause)
	} else {
		return "", fmt.Errorf("source sql or table is required")
	}

	return query, nil
}

func (pgDialect) quoteIdentifier(identifier string) string {
	if isAlreadyQuotedIdentifier(identifier) {
		return identifier
	}
	return `"` + identifier + `"`
}

func (pgDialect) wrapError(err error) error {
	return util.WrapPgError(err)
}

func (pgDialect) getColumnHandler(dbType string) ColHandler {
	upper := strings.ToUpper(dbType)

	// PostgreSQL 数组类型在 pgx 中统一以 "_" 前缀命名（_int4、_text、_timestamp、_numeric ...），
	// 因此 DatabaseTypeName 对数组列会返回形如 "_INT4"、"_TEXT" 的名称。
	// 显式识别数组并交给专用 handler，避免依赖 default 分支的隐式行为，
	// 也为将来定制数组序列化（例如转 JSON）预留唯一入口。
	if strings.HasPrefix(upper, "_") {
		return pgArrayColumnHandler
	}

	switch upper {

	case "TIMESTAMP", "TIMESTAMPTZ", "DATETIME", "DATE", "TIME", "TIMETZ":
		return func(v any) string {
			if v == nil {
				return util.NullSentinel
			}
			if t, ok := v.(time.Time); ok {
				return t.Format("2006-01-02 15:04:05.000000")
			}
			return defaultColumnHandler(v)
		}
	default:
		return defaultColumnHandler
	}
}

// pgArrayColumnHandler 显式处理 PostgreSQL 数组列。
//
// pgx stdlib 驱动以文本格式返回数组字面量（如 {1,2,3}、{"a,b",c}、{}），
// 这正是 PostgreSQL COPY 能识别的数组输入语法，因此原样透传即可保证往返闭合：
//   - nil（SQL NULL） → NullSentinel，COPY 识别为真正的 NULL
//   - 字面量字符串     → SanitizeString 做 CSV 转义（含逗号的 {1,2,3} 会被引号包裹）
//
// 前提：目标列须为相同元素类型的数组类型；否则字面量会作为普通文本入库。
// 若日后需要改变数组的落地形态（例如转成 jsonb 的 [1,2,3]），在此函数内改写即可。
func pgArrayColumnHandler(v any) string {
	return defaultColumnHandler(v)
}
