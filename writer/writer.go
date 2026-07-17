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
}
