package writer

import (
	"context"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// NewWriter 根据 target 构建 Writer：优先按 target.S3 分发到对象存储（parquet），
// 否则按 target 引用的数据库类型分发到对应数据库写入器。
// metaDB 指向存放 manager.job_data_sync 的 PostgreSQL（meta_db），仅对象存储目标使用，
// 未配置 meta_db 时传零值。
func NewWriter(target *config.TargetConfig, dbResolver config.DBResolver, s3Resolver config.S3Resolver, metaDB config.DBConfig, jobName string) (Writer, error) {
	if target == nil {
		return nil, util.NonRetryable(fmt.Errorf("target config is required"))
	}

	// 对象存储目标：走 parquet 序列化 + S3 上传。
	if strings.TrimSpace(target.S3) != "" {
		s3, ok := s3Resolver.Resolve(target.S3)
		if !ok {
			return nil, util.NonRetryable(fmt.Errorf("target s3 %q not found", target.S3))
		}
		return NewParquetWriter(s3, metaDB, target, jobName)
	}

	// 数据库目标：Resolve 返回共享 datasource 配置的值副本，下方改写只作用于本副本。
	db, ok := dbResolver.Resolve(target.ConnID, target.ConnName)
	if !ok {
		return nil, util.NonRetryable(fmt.Errorf("target db not found (conn_id=%q conn_name=%q)", target.ConnID, target.ConnName))
	}

	switch db.Type {
	case config.DBTypePG, config.DBTypeGP:
		// initial（首次全量）模式单次可能写入上亿行，COPY 耗时较长。
		// 未显式配置 statement_timeout（0）时，采用 2 小时的宽松默认，避免误触发超时。
		if target.Mode == config.ModeTypeInitial && db.StatementTimeout == 0 {
			db.StatementTimeout = config.InitialModeDefaultTimeoutSec
		}

		// lock_timeout、statement_timeout、TimeZone 均已由 db.DSN() 注入连接串。
		pgConn, err := pgx.Connect(context.Background(), db.DSN())
		if err != nil {
			return nil, fmt.Errorf("PG connect failed: %w", err)
		}
		// 此处刻意不传 metaDB：PG/GP 目标的水位由 upsertWatermark 写在目标库自己的事务里，
		// 与数据落地同进同退（见 watermark.go 中 pgxExecutor 的说明）；
		// 换成独立的 metaDB 连接会破坏这个原子性，代价是 manager.job_data_sync 必须与目标库同实例。
		// 对象存储无事务可挂靠，才需要单独的 metaDB 连接（见 NewParquetWriter）。
		return NewPGWriter(pgConn, target, jobName), nil
	default:
		// 不支持的类型属配置错误，重试无益。
		return nil, util.NonRetryable(fmt.Errorf("unsupported target db type: %s", db.Type))
	}
}
