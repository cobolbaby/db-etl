package reader

import "context"

// RowBatch 是一批原始行：值保持驱动返回的 Go 类型，不做任何面向目标格式的序列化。
type RowBatch struct {
	Rows [][]any
}

type Reader interface {
	// ReadBatch 异步抽取数据并返回 RowBatch channel。
	// 若抽取期间出错，会记录错误（见 Err）并调用 cancel 通知下游中止，
	// 避免 writer 提交被截断的部分数据。
	ReadBatch(ctx context.Context, cancel context.CancelFunc) <-chan RowBatch

	// GetColumnMeta 返回每列的列名、源库类型名与归一化语义类别，顺序与查询列一致。
	GetColumnMeta() ([]ColumnMeta, error)

	// Err 返回 ReadBatch 异步读取期间发生的错误（如查询语法错误）。
	// 应在消费完 ReadBatch 返回的 channel 之后调用。
	Err() error

	// Close 释放底层数据库连接。应在每次 pipeline 结束（含重试的每一轮）后调用，
	// 避免重试重建连接时泄漏。
	Close() error
}
