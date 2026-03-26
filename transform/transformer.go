package transform

import "db-etl/reader"

type CSVBatch struct {
	Rows [][]string
}

type Transformer interface {
	// RowBatch -> CSVBatch
	Transform(batch reader.RowBatch) CSVBatch
}
