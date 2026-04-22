package transform

import "db-etl/reader"

type CSVBatch struct {
	Columns []string
	Rows [][]string
}

type Transformer interface {
	// RowBatch -> CSVBatch
	Transform(batch reader.RowBatch) CSVBatch
}

type DefaultTransformer struct {
	Handlers []reader.ColHandler
}

func (t *DefaultTransformer) Transform(batch reader.RowBatch) CSVBatch {
	res := make([][]string, len(batch.Rows))
	for i, row := range batch.Rows {
		rec := make([]string, len(row))
		for j, v := range row {
			rec[j] = t.Handlers[j](v)
		}
		res[i] = rec
	}
	return CSVBatch{Columns: batch.Columns, Rows: res}
}
