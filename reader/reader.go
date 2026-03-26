package reader

import "database/sql"

type RowBatch struct {
	Rows [][]any
}

type Reader interface {
	// 返回 RowBatch channel
	ReadBatch() <-chan RowBatch

	// 新增接口
	GetColumnTypes() ([]*sql.ColumnType, error)
}
