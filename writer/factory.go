package writer

import (
	"context"
	"db-etl/config"
	"fmt"
	"log"

	"github.com/jackc/pgx/v5"
)

func NewWriter(db config.DBConfig, target *config.TargetConfig, jobName string) Writer {
	switch db.Type {
	case config.DBTypePG, config.DBTypeGP:
		cfg, err := pgx.ParseConfig(db.DSN())
		if err != nil {
			log.Fatalf("PG parse config failed: %v", err)
		}

		// statement_timeout 配置为 0 时不注入，保持服务器默认值
		if db.StatementTimeout > 0 {
			if cfg.RuntimeParams == nil {
				cfg.RuntimeParams = make(map[string]string)
			}
			cfg.RuntimeParams["statement_timeout"] = fmt.Sprintf("%d", db.StatementTimeout*1000) // ms
		}

		// 固定会话时区，确保无时区时间字符串写入 timestamptz 列时被确定性解析，
		// 不随运行环境（PGTZ/TZ/服务端默认）漂移。为空时不注入，保持服务端默认。
		if db.TimeZone != "" {
			if cfg.RuntimeParams == nil {
				cfg.RuntimeParams = make(map[string]string)
			}
			cfg.RuntimeParams["TimeZone"] = db.TimeZone
		}

		pgConn, err := pgx.ConnectConfig(context.Background(), cfg)
		if err != nil {
			log.Fatalf("PG connect failed: %v", err)
		}
		return NewPGWriter(pgConn, target, jobName)
	default:
		log.Fatalf("unsupported target db type: %s", db.Type)
	}
	return nil
}
