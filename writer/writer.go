package writer

import (
	"db-etl/config"
	"db-etl/transform"
)

type BatchWriter interface {
	// 从 channel 写入目标数据库
	WriteBatch(source *config.SourceConfig, in <-chan transform.CSVBatch) error
}

type WatermarkStore interface {
	GetWatermark(source *config.SourceConfig) (string, error)
}

type Writer interface {
	BatchWriter
	WatermarkStore
}
