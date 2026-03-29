package reader

import (
	"database/sql"
	"db-etl/util"
	"fmt"
	"log"
	"strings"
	"time"
)

type PGReader struct {
	DB        *sql.DB
	SQL       string
	Table     string
	BatchSize int
}

func (r *PGReader) ReadBatch() <-chan RowBatch {
	out := make(chan RowBatch, 8)
	go func() {
		defer close(out)

		var query string
		if r.SQL != "" {
			query = r.SQL
		} else if r.Table != "" {
			query = "SELECT * FROM " + r.Table
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
	if r.SQL != "" {
		query = fmt.Sprintf("SELECT * FROM (%s) t WHERE 1=0", r.SQL)
	} else {
		query = fmt.Sprintf("SELECT * FROM %s WHERE 1=0", r.Table)
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

	case "DATETIME", "DATETIME2", "DATE", "TIME":
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
