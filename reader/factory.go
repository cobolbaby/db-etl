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
)

// pingTimeout 为工厂建连后主动探活的超时时间。
const pingTimeout = 10 * time.Second

func NewReader(db config.DBConfig, src *config.SourceConfig) (Reader, error) {
	queryTimeout := time.Duration(db.QueryTimeout) * time.Second

	var (
		driver string
		build  func(*sql.DB) Reader
	)
	switch db.Type {
	case config.DBTypeMSSQL:
		driver = "sqlserver"
		build = func(conn *sql.DB) Reader { return NewMSSQLReader(conn, src, queryTimeout) }
	case config.DBTypePG, config.DBTypeGP:
		driver = "pgx"
		build = func(conn *sql.DB) Reader { return NewPGReader(conn, src, queryTimeout) }
	default:
		// 不支持的类型属配置错误，重试无益。
		return nil, util.NonRetryable(fmt.Errorf("unsupported source db type: %s", db.Type))
	}

	conn, err := sql.Open(driver, db.DSN())
	if err != nil {
		// sql.Open 仅校验参数、不建连，失败属配置错误，不可重试。
		return nil, util.NonRetryable(fmt.Errorf("open %s connection failed: %w", driver, err))
	}

	// sql.Open 是惰性的，不会真正建连；用 Ping 主动探活，
	// 使「数据库连不上」在此即被捕获并作为可重试错误上抛，交由上层重试机制处理。
	ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
	defer cancel()
	if err := conn.PingContext(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping %s db failed: %w", driver, err)
	}

	return build(conn), nil
}
