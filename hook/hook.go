package hook

import (
	"context"
	"db-etl/config"
	"fmt"
	"log"
	"strings"

	"github.com/jackc/pgx/v5"
)

// Executor executes SQL hooks.
type Executor struct {
	resolver config.DBResolver
}

// NewExecutor creates a new hook executor.
func NewExecutor(resolver config.DBResolver) *Executor {
	return &Executor{resolver: resolver}
}

// RunPreHooks executes all pre-hooks before data synchronization.
func (e *Executor) RunPreHooks(hooks []config.HookConfig) error {
	return e.runHooks(hooks, "pre")
}

// RunPostHooks executes all post-hooks after data synchronization.
func (e *Executor) RunPostHooks(hooks []config.HookConfig) error {
	return e.runHooks(hooks, "post")
}

func (e *Executor) runHooks(hooks []config.HookConfig, hookType string) error {
	ctx := context.Background()

	for i, h := range hooks {
		if strings.TrimSpace(h.ConnName) == "" {
			return fmt.Errorf("%s hook #%d: conn_name is required", hookType, i+1)
		}
		if strings.TrimSpace(h.SQL) == "" {
			return fmt.Errorf("%s hook #%d: sql is empty", hookType, i+1)
		}

		dbCfg, ok := e.resolver.Resolve("", h.ConnName)
		if !ok {
			return fmt.Errorf("%s hook #%d: conn_name %q not found", hookType, i+1, h.ConnName)
		}

		conn, err := e.connectDB(ctx, dbCfg)
		if err != nil {
			return fmt.Errorf("%s hook #%d: connect failed: %w", hookType, i+1, err)
		}

		log.Printf("running %s hook #%d on %s: %s", hookType, i+1, dbCfg.Name, shortenSQL(h.SQL))
		tag, err := conn.Exec(ctx, h.SQL)
		conn.Close(ctx)

		if err != nil {
			return fmt.Errorf("%s hook #%d failed: %w", hookType, i+1, err)
		}
		log.Printf("%s hook #%d completed: rows=%d", hookType, i+1, tag.RowsAffected())
	}
	return nil
}

func (e *Executor) connectDB(ctx context.Context, dbCfg config.DBConfig) (*pgx.Conn, error) {
	switch dbCfg.Type {
	case config.DBTypePG, config.DBTypeGP:
		return pgx.Connect(ctx, dbCfg.DSN())
	default:
		return nil, fmt.Errorf("unsupported db type: %s", dbCfg.Type)
	}
}

func shortenSQL(sql string) string {
	if len(sql) <= 100 {
		return sql
	}
	return sql[:100] + "..."
}
