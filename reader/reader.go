package reader

import "context"

// Batch 是贯穿 reader → transform → writer 的唯一数据载体：列元数据 + 驱动返回的原始值。
// 列元数据随批次同行，让链路上每一段都能自描述地知道「这批数据有哪些列、是什么语义类别」，
// 无需额外的带外探测；transform 重塑列结构（如 unpivot）时也只需返回新的 Batch。
//
// 值不在链路中途序列化 —— 文本编码是写入端的格式细节（PG COPY 要 CSV 文本，
// parquet 要类型化的列值），提前转成字符串会迫使 parquet 再解析回去，既有开销也有精度损失。
type Batch struct {
	Columns []ColumnMeta
	Rows    [][]any
}

// ColumnNames 提取列名切片，供只需列名的 writer（如 PG COPY 构建 SQL）使用。
func (b Batch) ColumnNames() []string {
	names := make([]string, len(b.Columns))
	for i, c := range b.Columns {
		names[i] = c.Name
	}
	return names
}

type Reader interface {
	// ReadBatch 异步抽取数据并返回 Batch channel，每个批次都带有该结果集的列元数据。
	// 若抽取期间出错，会记录错误（见 Err）并调用 cancel 通知下游中止，
	// 避免 writer 提交被截断的部分数据。
	ReadBatch(ctx context.Context, cancel context.CancelFunc) <-chan Batch

	// Err 返回 ReadBatch 异步读取期间发生的错误（如查询语法错误）。
	// 应在消费完 ReadBatch 返回的 channel 之后调用。
	Err() error

	// Close 释放底层数据库连接。应在每次 pipeline 结束（含重试的每一轮）后调用，
	// 避免重试重建连接时泄漏。
	Close() error
}
