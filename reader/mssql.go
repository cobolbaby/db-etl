package reader

import (
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type MSSQLReader struct {
	*BaseReader
}

type mssqlDialect struct{}

func NewMSSQLReader(db *sql.DB, src *config.SourceConfig) Reader {
	return &MSSQLReader{
		BaseReader: &BaseReader{
			conn:    db,
			Source:  src,
			dialect: mssqlDialect{},
		},
	}
}

func (mssqlDialect) buildBaseQuery(source *config.SourceConfig, projection string, whereClause string) (string, error) {
	var query string

	if source.SQL != "" {
		// Stored procedures (EXEC/EXECUTE) cannot be wrapped in subquery like SELECT * FROM (EXEC ...) t
		// because SQL Server doesn't support this syntax. Execute them directly without wrapping.
		if isStoredProcedure(source.SQL) {
			query = source.SQL
		} else {
			query = fmt.Sprintf("SELECT %s FROM (%s) t WHERE %s", projection, source.SQL, whereClause)
		}
	} else if source.Table != "" {
		query = fmt.Sprintf("SELECT %s FROM %s WITH (NOLOCK) WHERE %s", projection, source.Table, whereClause)
	} else {
		return "", fmt.Errorf("source sql or table is required")
	}

	return query, nil
}

// isStoredProcedure checks if the SQL statement is a stored procedure call (starts with EXEC or EXECUTE).
func isStoredProcedure(sql string) bool {
	sql = strings.TrimSpace(sql)
	return strings.HasPrefix(strings.ToUpper(sql), "EXEC") ||
		strings.HasPrefix(strings.ToUpper(sql), "EXECUTE")
}

func (mssqlDialect) quoteIdentifier(identifier string) string {
	if isAlreadyQuotedIdentifier(identifier) {
		return identifier
	}
	return "[" + identifier + "]"
}

func (mssqlDialect) wrapError(err error) error {
	return util.WrapMSSQLError(err)
}

// columnKind 将 SQL Server 的类型名映射到归一化语义类别。
func (mssqlDialect) columnKind(dbType string) ColumnKind {
	switch strings.ToUpper(dbType) {
	case "BIT":
		return KindBool
	case "TINYINT", "SMALLINT", "INT", "BIGINT":
		return KindInt
	case "REAL", "FLOAT":
		return KindFloat
	case "DATETIME", "DATETIME2", "SMALLDATETIME", "DATE", "TIME", "DATETIMEOFFSET":
		return KindTime
	case "BINARY", "VARBINARY", "IMAGE", "TIMESTAMP", "ROWVERSION":
		// 注意：SQL Server 的 TIMESTAMP 是行版本戳（8 字节二进制），与时间无关。
		return KindBytes
	default:
		// DECIMAL/NUMERIC/MONEY 按字符串透传以免精度损失；
		// UNIQUEIDENTIFIER 经 valueNormalizer 归一为 UUID 文本；其余文本类同理。
		return KindString
	}
}

// valueNormalizer 修正 uniqueidentifier 的字节序。
// go-mssqldb 以 SQL Server 的混合字节序返回该类型的 16 字节值，
// 需按 RFC 4122 重排后才是通用的 UUID 文本。
func (mssqlDialect) valueNormalizer(dbType string) ValueNormalizer {
	if strings.ToUpper(dbType) != "UNIQUEIDENTIFIER" {
		return nil
	}
	return func(v any) any {
		switch t := v.(type) {
		case []byte:
			s, err := MSSQLUUIDToString(t)
			if err != nil {
				return v
			}
			return s
		case string:
			return strings.ToUpper(t)
		default:
			return v
		}
	}
}

func MSSQLUUIDToString(b []byte) (string, error) {
	if len(b) != 16 {
		return "", fmt.Errorf("invalid uuid length")
	}
	u := []byte{
		b[3], b[2], b[1], b[0],
		b[5], b[4],
		b[7], b[6],
		b[8], b[9],
		b[10], b[11], b[12], b[13], b[14], b[15],
	}
	id, _ := uuid.FromBytes(u)
	return strings.ToUpper(id.String()), nil
}
