package reader

import (
	"context"
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
)

func NewReader(db config.DBConfig, src *config.SourceConfig) (Reader, error) {
	// initial（首次全量）模式单次可能回填上亿行，顺扫耗时较长。
	// 未显式配置 statement_timeout（0）时，采用 2 小时的宽松默认，避免误触发超时；
	// 其他模式不覆盖，延用数据库端默认配置。
	if src != nil && src.Mode == config.ModeTypeInitial && db.StatementTimeout == 0 {
		db.StatementTimeout = config.InitialModeDefaultTimeoutSec
	}

	// loc 由源库配置的 timezone 解析而来，供把无时区时间列的墙钟解释为源库所在时区。
	// 解析失败属配置错误，重试无益。
	loc, err := db.Location()
	if err != nil {
		return nil, util.NonRetryable(fmt.Errorf("parse source timezone failed: %w", err))
	}

	var build func(*sql.DB) Reader
	switch db.Type {
	case config.DBTypeMSSQL:
		build = func(conn *sql.DB) Reader { return NewMSSQLReader(conn, src, loc) }
	case config.DBTypePG, config.DBTypeGP:
		build = func(conn *sql.DB) Reader { return NewPGReader(conn, src, loc) }
	case config.DBTypeOracle:
		build = func(conn *sql.DB) Reader { return NewOracleReader(conn, src, loc) }
	default:
		// 不支持的类型属配置错误，重试无益。
		return nil, util.NonRetryable(fmt.Errorf("unsupported source db type: %s", db.Type))
	}

	conn, err := sql.Open(db.Driver(), db.DSN())
	if err != nil {
		// sql.Open 仅校验参数、不建连，失败属配置错误，不可重试。
		return nil, util.NonRetryable(fmt.Errorf("open connection failed: %w", err))
	}

	// sql.Open 是惰性的，不会真正建连；用 Ping 主动探活，
	// 使「数据库连不上」在此即被捕获并作为可重试错误上抛，交由上层重试机制处理。
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(db.PingTimeout)*time.Second)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping db failed: %w", err)
	}

	return build(conn), nil
}
