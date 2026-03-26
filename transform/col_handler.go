package transform

import (
	"db-etl/reader"
	"db-etl/util"
)

type DefaultTransformer struct {
	Handlers []util.ColHandler
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
	return CSVBatch{Rows: res}
}
