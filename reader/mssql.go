package reader

import (
	"database/sql"
	"db-etl/config"
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

func (mssqlDialect) buildBaseQuery(source *config.SourceConfig, emptyResult bool) (string, error) {
	var query string
	whereClause := "1=1"
	if emptyResult {
		whereClause = "1=0"
	}

	if source.SQL != "" {
		query = fmt.Sprintf("SELECT * FROM (%s) t WHERE %s", source.SQL, whereClause)
	} else if source.Table != "" {
		query = fmt.Sprintf("SELECT * FROM %s WITH (NOLOCK) WHERE %s", source.Table, whereClause)
	} else {
		return "", fmt.Errorf("source sql or table is required")
	}

	return query, nil
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
				return ""
			}
		}
	case "DATETIME", "DATETIME2", "DATE", "TIME":
		return func(v any) string {
			if t, ok := v.(time.Time); ok && !t.IsZero() {
				return t.Format("2006-01-02 15:04:05.000")
			}
			return ""
		}
	case "IMAGE", "VARBINARY", "BINARY":
		// PostgreSQL bytea 在 COPY CSV 中使用 \x 十六进制格式
		return func(v any) string {
			if b, ok := v.([]byte); ok && len(b) > 0 {
				return `\x` + hex.EncodeToString(b)
			}
			return ""
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
