package reader

import (
	"database/sql"
	"db-etl/config"
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

func (pgDialect) buildBaseQuery(source *config.SourceConfig, emptyResult bool) (string, error) {
	var query string
	whereClause := "1=1"
	if emptyResult {
		whereClause = "1=0"
	}

	if source.SQL != "" {
		query = fmt.Sprintf("SELECT * FROM (%s) t WHERE %s", source.SQL, whereClause)
	} else if source.Table != "" {
		query = fmt.Sprintf("SELECT * FROM %s WHERE %s", source.Table, whereClause)
	} else {
		return "", fmt.Errorf("source sql or table is required")
	}

	return query, nil
}

func (pgDialect) getColumnHandler(dbType string) ColHandler {
	switch strings.ToUpper(dbType) {

	case "DATETIME", "DATE", "TIME":
		return func(v any) string {
			if t, ok := v.(time.Time); ok && !t.IsZero() {
				return t.Format("2006-01-02 15:04:05.000")
			}
			return ""
		}
	default:
		return defaultColumnHandler
	}
}
