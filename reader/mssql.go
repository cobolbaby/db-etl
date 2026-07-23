package reader

import (
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type MSSQLReader struct {
	*BaseReader
}

type mssqlDialect struct{}

func NewMSSQLReader(db *sql.DB, src *config.SourceConfig) Reader {
	return &MSSQLReader{
		BaseReader: &BaseReader{
			DB:      db,
			Source:  src,
			dialect: mssqlDialect{},
		},
	}
}

func (mssqlDialect) buildBaseQuery(source *config.SourceConfig, projection string, whereClause string) (string, error) {
	var query string

	if source.SQL != "" {
		query = fmt.Sprintf("SELECT %s FROM (%s) t WHERE %s", projection, source.SQL, whereClause)
	} else if source.Table != "" {
		query = fmt.Sprintf("SELECT %s FROM %s WITH (NOLOCK) WHERE %s", projection, source.Table, whereClause)
	} else {
		return "", fmt.Errorf("source sql or table is required")
	}

	return query, nil
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

func (mssqlDialect) getColumnHandler(dbType string) ColHandler {
	switch strings.ToUpper(dbType) {
	case "UNIQUEIDENTIFIER":
		return func(v any) string {
			switch t := v.(type) {
			case []byte:
				s, _ := MSSQLUUIDToString(t)
				return s
			case string:
				return strings.ToUpper(t)
			default:
				return defaultColumnHandler(v)
			}
		}
	case "DATETIME", "DATETIME2", "DATE", "TIME":
		return func(v any) string {
			if v == nil {
				return util.NullSentinel
			}
			if t, ok := v.(time.Time); ok {
				return t.Format("2006-01-02 15:04:05.000000")
			}
			return defaultColumnHandler(v)
		}
	case "IMAGE", "VARBINARY", "BINARY":
		// PostgreSQL bytea 在 COPY CSV 中使用 \x 十六进制格式
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
	default:
		return defaultColumnHandler
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
