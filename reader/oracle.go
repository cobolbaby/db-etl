package reader

import (
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"strings"
	"time"
)

type OracleReader struct {
	*BaseReader
}

type oracleDialect struct{}

func NewOracleReader(db *sql.DB, src *config.SourceConfig, loc *time.Location) Reader {
	return &OracleReader{
		BaseReader: &BaseReader{
			conn:    db,
			Source:  src,
			dialect: oracleDialect{},
			loc:     loc,
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

// columnKind 将 Oracle 的类型名映射到归一化语义类别。
func (oracleDialect) columnKind(dbType string) ColumnKind {
	upper := strings.ToUpper(dbType)

	// 二进制类型（RAW / LongRaw / BLOB）：go-ora 以 []byte 返回，
	// 默认 LOB 内联模式下 BLOB 列的类型名会呈现为 LONGRAW。
	if strings.Contains(upper, "RAW") || strings.Contains(upper, "BLOB") {
		return KindBytes
	}

	// DATE、TIMESTAMP、TIMESTAMP WITH [LOCAL] TIME ZONE 等在类型名中均含 DATE/TIMESTAMP/TIMETZ，
	// go-ora 一律以 time.Time 返回。
	if upper == "DATE" || strings.Contains(upper, "TIMESTAMP") || strings.Contains(upper, "TIMETZ") {
		return KindTime
	}

	// NUMBER 由 go-ora 以字符串返回（完整精度，无 float64 精度损失），按字符串透传；
	// VARCHAR2/CHAR/NCHAR/CLOB/LONG 等文本类同理。
	return KindString
}

// valueNormalizer Oracle 驱动返回的值已符合各 ColumnKind 的约定，无需修正。
func (oracleDialect) valueNormalizer(string, *time.Location) ValueNormalizer { return nil }
