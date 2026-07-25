package writer

import (
	"context"
	"db-etl/config"
	"db-etl/util"
	"fmt"

	"github.com/jackc/pgx/v5"
)

func NewWriter(db config.DBConfig, target *config.TargetConfig, jobName string) (Writer, error) {
	switch db.Type {
	case config.DBTypePG, config.DBTypeGP:
		// initial（首次全量）模式单次可能写入上亿行，COPY 耗时较长。
		// 未显式配置 statement_timeout（0）时，采用 2 小时的宽松默认，避免误触发超时。
		// 该参数已通过 db.DSN() 注入到连接串中。
		if target != nil && target.Mode == config.ModeTypeInitial && db.StatementTimeout == 0 {
			db.StatementTimeout = config.InitialModeDefaultTimeoutSec
		}

		// pgx.Connect 直接接受 DSN 字符串，所有参数（lock_timeout、statement_timeout、TimeZone）
		// 均已在 db.DSN() 中注入，此处无需额外处理。
		pgConn, err := pgx.Connect(context.Background(), db.DSN())
		if err != nil {
			return nil, fmt.Errorf("PG connect failed: %w", err)
		}
		return NewPGWriter(pgConn, target, jobName), nil
	default:
		// 不支持的类型属配置错误，重试无益。
		return nil, util.NonRetryable(fmt.Errorf("unsupported target db type: %s", db.Type))
	}
}
