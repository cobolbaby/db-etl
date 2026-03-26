package reader

import (
	"database/sql"
	"log"
)

type MSSQLReader struct {
	DB        *sql.DB
	Table     string
	BatchSize int
}

func (r *MSSQLReader) ReadBatch() <-chan RowBatch {
	out := make(chan RowBatch, 8)
	go func() {
		defer close(out)
		rows, err := r.DB.Query("SELECT * FROM " + r.Table + " WITH (NOLOCK)")
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

func (r *MSSQLReader) GetColumnTypes() ([]*sql.ColumnType, error) {

	query := "SELECT * FROM " + r.Table + " WHERE 1=0" // 只获取列信息，不返回数据

	rows, err := r.DB.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows.ColumnTypes()
}
