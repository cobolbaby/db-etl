package reader

import (
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/google/uuid"
)

type MSSQLReader struct {
	DB        *sql.DB
	Src       *config.SourceConfig
	BatchSize int
}

func (r *MSSQLReader) ReadBatch() <-chan RowBatch {
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

func (r *MSSQLReader) GetColumnHandlers() []ColHandler {
	colTypes, _ := r.getColumnTypes()
	handlers := make([]ColHandler, len(colTypes))
	for i, ct := range colTypes {
		handlers[i] = r.getColumnHandler(ct.DatabaseTypeName())
	}
	return handlers
}

func (r *MSSQLReader) getColumnTypes() ([]*sql.ColumnType, error) {

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

func (r *MSSQLReader) getColumnHandler(dbType string) ColHandler {
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
