package reader

import "context"

type RowBatch struct {
	Columns []string
	Rows [][]any
}

type ColHandler func(any) string

type Reader interface {
	// ReadBatch 异步抽取数据并返回 RowBatch channel。
	// 若抽取期间出错，会记录错误（见 Err）并调用 cancel 通知下游中止，
	// 避免 writer 提交被截断的部分数据。
	ReadBatch(ctx context.Context, cancel context.CancelFunc) <-chan RowBatch

	GetColumnHandlers() ([]ColHandler, error)

	// Err 返回 ReadBatch 异步读取期间发生的错误（如查询语法错误）。
	// 应在消费完 ReadBatch 返回的 channel 之后调用。
	Err() error

	// Close 释放底层数据库连接。应在每次 pipeline 结束（含重试的每一轮）后调用，
	// 避免重试重建连接时泄漏。
	Close() error
}
