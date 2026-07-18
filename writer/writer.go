package writer

import (
	"context"

	"db-etl/config"
	"db-etl/transform"
)

type BatchWriter interface {
	// 从 channel 写入目标数据库。ctx 被取消时（如 reader 出错）应中止写入并回滚。
	WriteBatch(ctx context.Context, source *config.SourceConfig, in <-chan transform.CSVBatch) error
}

type WatermarkStore interface {
	GetWatermark(source *config.SourceConfig) (string, error)
}

type Writer interface {
	BatchWriter
	WatermarkStore

	// Close 释放底层数据库连接。应在每次 pipeline 结束（含重试的每一轮）后调用，
	// 避免重试重建连接时泄漏。
	Close() error
}
