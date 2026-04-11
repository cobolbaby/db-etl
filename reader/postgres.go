package reader

import (
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"log"
	"strings"
	"time"
)

type PGReader struct {
	DB        *sql.DB
	Src       *config.SourceConfig
	BatchSize int
}

func (r *PGReader) ReadBatch() <-chan RowBatch {
	out := make(chan RowBatch, 8)
	go func() {
		defer close(out)

		var query string
		if r.Src.SQL != "" {
			query = r.Src.SQL
		} else if r.Src.Table != "" {
			query = "SELECT * FROM " + r.Src.Table + " WHERE 1=1"
		}

		// 如果是增量抽取，替换掉 SQL 中的占位符
		if r.Src.Mode != config.ModeTypeFull && r.Src.IncrField != "" {
			query = query + fmt.Sprintf(" AND %s > %s", r.Src.IncrField, r.Src.IncrPoint)
		}

		rows, err := r.DB.Query(query)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		cols, _ := rows.Columns()
		for {
			batch := make([][]any, 0, r.BatchSize)
			for len(batch) < r.BatchSize && rows.Next() {
				values := make([]any, len(cols))
				valuePtrs := make([]any, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				rows.Scan(valuePtrs...)
				batch = append(batch, values)
			}
			if len(batch) == 0 {
				break
			}
			out <- RowBatch{Rows: batch}
		}
	}()
	return out
}

func (r *PGReader) GetColumnHandlers() []ColHandler {
	colTypes, _ := r.getColumnTypes()
	handlers := make([]ColHandler, len(colTypes))
	for i, ct := range colTypes {
		handlers[i] = r.getColumnHandler(ct.DatabaseTypeName())
	}
	return handlers
}

func (r *PGReader) getColumnTypes() ([]*sql.ColumnType, error) {

	var query string
	if r.Src.SQL != "" {
		query = fmt.Sprintf("SELECT * FROM (%s) t WHERE 1=0", r.Src.SQL)
	} else if r.Src.Table != "" {
		query = fmt.Sprintf("SELECT * FROM %s WHERE 1=0", r.Src.Table)
	}
	rows, err := r.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows.ColumnTypes()
}

func (r *PGReader) getColumnHandler(dbType string) ColHandler {
	switch strings.ToUpper(dbType) {

	case "DATETIME", "DATE", "TIME":
		return func(v any) string {
			if t, ok := v.(time.Time); ok && !t.IsZero() {
				return t.Format("2006-01-02 15:04:05")
			}
			return ""
		}
	default:
		return func(v any) string {
			if v == nil {
				return ""
			}
			switch t := v.(type) {
			case []byte:
				return util.SanitizeString(string(t))
			case string:
				return util.SanitizeString(t)
			default:
				return util.SanitizeString(fmt.Sprintf("%v", t))
			}
		}
	}
}
