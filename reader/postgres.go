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

func NewPGReader(db *sql.DB, src *config.SourceConfig, loc *time.Location) Reader {
	return &PGReader{
		BaseReader: &BaseReader{
			conn:    db,
			Source:  src,
			dialect: pgDialect{},
			loc:     loc,
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

// columnKind 将 PostgreSQL / Greenplum 的类型名映射到归一化语义类别。
func (pgDialect) columnKind(dbType string) ColumnKind {
	upper := strings.ToUpper(dbType)

	// PostgreSQL 数组类型在 pgx 中统一以 "_" 前缀命名（_int4、_text、_timestamp、_numeric ...）。
	// pgx stdlib 以文本字面量（如 {1,2,3}）返回数组，正是 COPY 能识别的数组输入语法，
	// 故按字符串原样透传；前提是目标列为相同元素类型的数组。
	if strings.HasPrefix(upper, "_") {
		return KindString
	}

	switch upper {
	case "BOOL":
		return KindBool
	case "INT2", "INT4", "INT8", "SMALLINT", "INTEGER", "BIGINT",
		"SERIAL", "BIGSERIAL", "SMALLSERIAL":
		return KindInt
	case "FLOAT4", "FLOAT8", "REAL", "DOUBLE PRECISION":
		return KindFloat
	case "TIMESTAMP", "TIMESTAMPTZ", "DATE", "TIME", "TIMETZ":
		return KindTime
	case "BYTEA":
		return KindBytes
	default:
		// NUMERIC/DECIMAL/MONEY 由 pgx 以文本返回，按字符串透传以免精度损失；
		// VARCHAR/TEXT/UUID/JSON/JSONB 等文本类同理。
		return KindString
	}
}

// valueNormalizer 修正驱动层的值表示差异。
// 无时区日期时间（TIMESTAMP/DATE/TIME）：pgx 以「墙钟 + UTC Location」返回，
// 贴回本地时区，避免下游按 UTC 归一时整体偏移时差。
// TIMESTAMPTZ / TIMETZ 自带时区、驱动已返回正确瞬时，故不在此列；其余类型无需修正。
func (pgDialect) valueNormalizer(dbType string, loc *time.Location) ValueNormalizer {
	switch strings.ToUpper(dbType) {
	case "TIMESTAMP", "DATE", "TIME":
		return NaiveTimeNormalizer(loc)
	default:
		return nil
	}
}
