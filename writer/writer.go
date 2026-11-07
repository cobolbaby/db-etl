package writer

import (
	"context"

	"db-etl/config"
	"db-etl/reader"
)

type BatchWriter interface {
	// 从 channel 写入目标数据库。ctx 被取消时（如 reader 出错）应中止写入并回滚。
	WriteBatch(ctx context.Context, source *config.SourceConfig, in <-chan reader.Batch) error
}

type WatermarkStore interface {
	GetWatermark(source *config.SourceConfig) (string, error)
	// FinalizeSyncResult 登记最近一次同步结果到 manager.job_data_sync.last_status
	// （成功 0 / 失败 1，best-effort）；once=true 且本次成功时顺带将 inuse 置为 false。
	FinalizeSyncResult(source *config.SourceConfig, status int, once bool) error
}

type Writer interface {
	BatchWriter
	WatermarkStore

	// TruncateTarget 独立执行一次目标清空，不写入任何数据。供多 source 合并写同一
	// 目标表的 full 模式任务在 source 循环开始前调用一次（见 main.go runTask），
	// 避免每个 source 各自独立清空、互相冲掉对方刚写入的数据。
	// DB 目标执行 TRUNCATE/DELETE；对象存储目标为空操作（覆盖写单对象已天然等效）。
	TruncateTarget(ctx context.Context) error

	// Close 释放底层数据库连接。应在每次 pipeline 结束（含重试的每一轮）后调用，
	// 避免重试重建连接时泄漏。
	Close() error
}
