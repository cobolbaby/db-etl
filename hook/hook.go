package hook

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"db-etl/config"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
)

// Executor 是 hook 执行器的抽象接口。
type Executor interface {
	Execute(ctx context.Context, spec map[string]any) error
}

// RunPreHooks executes all pre-hooks before data synchronization.
func RunPreHooks(ctx context.Context, hooks []config.HookConfig, resolver config.DBResolver) error {
	return runHooks(ctx, hooks, resolver, "pre")
}

// RunPostHooks executes all post-hooks after data synchronization.
func RunPostHooks(ctx context.Context, hooks []config.HookConfig, resolver config.DBResolver) error {
	return runHooks(ctx, hooks, resolver, "post")
}

func runHooks(ctx context.Context, hooks []config.HookConfig, resolver config.DBResolver, hookType string) error {
	for i, h := range hooks {
		exec, err := newExecutor(h.Type, resolver)
		if err != nil {
			return fmt.Errorf("%s hook #%d: %w", hookType, i+1, err)
		}
		if err := exec.Execute(ctx, h.Spec); err != nil {
			return fmt.Errorf("%s hook #%d: %w", hookType, i+1, err)
		}
	}
	return nil
}

// newExecutor 根据 hook 类型创建对应的 executor。
func newExecutor(hookType config.HookType, resolver config.DBResolver) (Executor, error) {
	switch hookType {
	case config.HookTypeSQL:
		return NewDBExecutor(resolver), nil
	default:
		return nil, fmt.Errorf("unsupported hook type: %q", hookType)
	}
}

// DBExecutor 通过数据库连接执行 SQL。
type DBExecutor struct {
	resolver config.DBResolver
}

// NewDBExecutor 创建 DBExecutor。
func NewDBExecutor(resolver config.DBResolver) *DBExecutor {
	return &DBExecutor{resolver: resolver}
}

// Execute 执行 SQL hook。
func (e *DBExecutor) Execute(ctx context.Context, spec map[string]any) error {
	connName, _ := spec["conn_name"].(string)
	sqlStmt, _ := spec["sql"].(string)

	dbCfg, ok := e.resolver.Resolve("", connName)
	if !ok {
		return fmt.Errorf("conn_name %q not found", connName)
	}

	db, err := sql.Open(dbCfg.Driver(), dbCfg.DSN())
	if err != nil {
		return fmt.Errorf("open connection failed: %w", err)
	}
	defer db.Close()

	pingCtx, pingCancel := context.WithTimeout(ctx, time.Duration(dbCfg.PingTimeout)*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("ping db failed: %w", err)
	}

	log.Printf("[hook] executing on %s: %s", connName, shortenSQL(sqlStmt))
	_, err = db.ExecContext(ctx, sqlStmt)
	if err != nil {
		return fmt.Errorf("exec failed: %w", err)
	}
	log.Printf("[hook] completed on %s: %s", connName, shortenSQL(sqlStmt))
	return nil
}

func shortenSQL(sql string) string {
	if len(sql) <= 100 {
		return sql
	}
	return sql[:100] + "..."
}
