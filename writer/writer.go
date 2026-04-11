package writer

import (
	"db-etl/config"
	"db-etl/transform"
)

type Writer interface {
	// 从 channel 写入目标数据库
	WriteBatch(in <-chan transform.CSVBatch) error

	GetWatermark(src *config.SourceConfig) (string, error)
}
