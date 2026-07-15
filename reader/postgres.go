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

func NewPGReader(db *sql.DB, src *config.SourceConfig, queryTimeout time.Duration) Reader {
	return &PGReader{
		BaseReader: &BaseReader{
			DB:           db,
			Source:       src,
			QueryTimeout: queryTimeout,
			dialect:      pgDialect{},
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

func (pgDialect) formatIncrValue(v string) string {
	s := strings.TrimSpace(v)
	if isNumericLiteral(s) {
		return s
	}
	return "'" + s + "'"
}

func (pgDialect) quoteIdentifier(identifier string) string {
	if isAlreadyQuotedIdentifier(identifier) {
		return identifier
	}
	return `"` + identifier + `"`
}

func (pgDialect) getColumnHandler(dbType string) ColHandler {
	switch strings.ToUpper(dbType) {

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
