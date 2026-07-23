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
		cfg, err := pgx.ParseConfig(db.DSN())
		if err != nil {
			// DSN 解析失败属配置错误，重试无益。
			return nil, util.NonRetryable(fmt.Errorf("PG parse config failed: %w", err))
		}

		// statement_timeout 配置为 0 时不注入，保持服务器默认值
		statementTimeout := db.StatementTimeout
		// initial（首次全量）模式单次可能写入上亿行，COPY 耗时较长。
		// 未显式配置 statement_timeout（0）时，采用 2 小时的宽松默认，避免误触发超时。
		if target != nil && target.Mode == config.ModeTypeInitial && statementTimeout == 0 {
			statementTimeout = config.InitialModeDefaultTimeoutSec
		}
		if statementTimeout > 0 {
			if cfg.RuntimeParams == nil {
				cfg.RuntimeParams = make(map[string]string)
			}
			cfg.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", statementTimeout*1000) // ms
		}

		// 固定会话时区，确保无时区时间字符串写入 timestamptz 列时被确定性解析，
		// 不随运行环境（PGTZ/TZ/服务端默认）漂移。为空时不注入，保持服务端默认。
		if db.TimeZone != "" {
			if cfg.RuntimeParams == nil {
				cfg.RuntimeParams = make(map[string]string)
			}
			cfg.RuntimeParams["TimeZone"] = db.TimeZone
		}

		// ConnectConfig 会真正建连；「连不上」返回可重试错误，交由上层重试机制处理。
		pgConn, err := pgx.ConnectConfig(context.Background(), cfg)
		if err != nil {
			return nil, fmt.Errorf("PG connect failed: %w", err)
		}
		return NewPGWriter(pgConn, target, jobName), nil
	default:
		// 不支持的类型属配置错误，重试无益。
		return nil, util.NonRetryable(fmt.Errorf("unsupported target db type: %s", db.Type))
	}
}
