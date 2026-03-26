package reader

type RowBatch struct {
	Rows [][]any
}

type ColHandler func(any) string

type Reader interface {
	// 返回 RowBatch channel
	ReadBatch() <-chan RowBatch

	GetColumnHandlers() []ColHandler
}
