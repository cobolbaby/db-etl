// Package sqljob 执行运维类 SQL 脚本任务，用于替代 `psql -f xxx.sql` / `sqlcmd -i xxx.sql` 这类脚本。
//
// 语句来源两种，可混用：
//   - blocks：内联在配置中的执行单元，可标注负责人/备注，便于维护；整块作为一个原子单元提交，不拆分；
//   - files ：外部 SQL 文件；与 blocks 均省略时，默认执行运行目录下所有 *.sql。
//     文件内可包含多段 SQL，按数据库方言拆分为多个执行单元：
//   - PostgreSQL/Greenplum/Oracle 以顶层分号 ';' 分隔（跳过字符串/注释/美元引用内部）；
//   - SQL Server 以单独成行的 GO 作为批处理分隔符。
//
// 每个执行单元串行执行、各自独立提交（构成不同事务）；重试粒度为单个执行单元：
// 某个失败仅重试该单元，前面已提交的单元不受影响。
package sqljob

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"db-etl/config"
	"db-etl/util"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
	_ "github.com/sijms/go-ora/v2"
)

// unit 是一个 SQL 执行单元：整段 SQL 作为一次 Exec 提交。
type unit struct {
	label string // 用于日志的名称（block 名或文件名）
	sql   string
}

// Run 执行一个 sqljob 任务：在解析到的连接上串行执行所有执行单元。
// 每个单元用 retryCfg 独立重试；任一单元最终失败则返回错误（后续单元不再执行）。
func Run(ctx context.Context, task config.TaskConfig, resolver config.DBResolver, retryCfg util.RetryConfig) error {
	cfg := task.SQLJob
	if cfg == nil {
		return fmt.Errorf("sqljob: task %q has no sqljob config", task.Name)
	}

	dbCfg, ok := resolver.Resolve(cfg.ConnID, cfg.ConnName)
	if !ok {
		return fmt.Errorf("sqljob: db not found (conn_id=%q conn_name=%q)", cfg.ConnID, cfg.ConnName)
	}

	units, err := collectUnits(cfg, dbCfg.Type)
	if err != nil {
		return fmt.Errorf("sqljob %q: %w", task.Name, err)
	}
	if len(units) == 0 {
		log.Printf("[sqljob] %q: no SQL to execute", task.Name)
		return nil
	}

	db, err := sql.Open(dbCfg.Driver(), dbCfg.DSN())
	if err != nil {
		return fmt.Errorf("sqljob %q: open connection failed: %w", task.Name, err)
	}
	defer db.Close()
	// 串行执行，单连接即可。
	db.SetMaxOpenConns(1)

	pingCtx, pingCancel := context.WithTimeout(ctx, time.Duration(dbCfg.PingTimeout)*time.Second)
	defer pingCancel()
	if err := db.PingContext(pingCtx); err != nil {
		return fmt.Errorf("sqljob %q: ping db failed: %w", task.Name, err)
	}

	log.Printf("[sqljob] %q: executing %d unit(s) on %s", task.Name, len(units), connLabel(dbCfg))

	for i, u := range units {
		label := fmt.Sprintf("sqljob %q unit %d/%d (%s)", task.Name, i+1, len(units), u.label)
		err := util.Retry(label, retryCfg, func() error {
			log.Printf("[sqljob] %s executing: %s", label, shortenSQL(u.sql))
			if _, err := db.ExecContext(ctx, u.sql); err != nil {
				return fmt.Errorf("exec failed: %w", err)
			}
			return nil
		})
		if err != nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		log.Printf("[sqljob] %s completed", label)
	}

	log.Printf("[sqljob] %q: all %d unit(s) completed", task.Name, len(units))
	return nil
}

// collectUnits 汇总执行单元：先 blocks，后 files。
// 当 blocks 与 files 均为空时，默认执行运行目录下所有 *.sql（按文件名排序）。
//
// blocks 作为整体是一个执行单元（原子提交），不做拆分；
// files 内可包含多段 SQL，按数据库方言拆分为多个执行单元，各自独立提交（不同事务）。
func collectUnits(cfg *config.SQLJobConfig, dbType config.DBType) ([]unit, error) {
	var units []unit

	for i, b := range cfg.Blocks {
		if strings.TrimSpace(b.SQL) == "" {
			continue
		}
		label := strings.TrimSpace(b.Name)
		if label == "" {
			label = fmt.Sprintf("block[%d]", i)
		}
		units = append(units, unit{label: label, sql: b.SQL})
	}

	files := cfg.Files
	if len(cfg.Blocks) == 0 && len(files) == 0 {
		matches, err := filepath.Glob("*.sql")
		if err != nil {
			return nil, fmt.Errorf("glob *.sql failed: %w", err)
		}
		sort.Strings(matches)
		files = matches
	}

	splitter := newSplitter(dbType)
	for _, f := range files {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		content, err := os.ReadFile(f)
		if err != nil {
			return nil, fmt.Errorf("read sql file %q failed: %w", f, err)
		}
		stmts := splitter.Split(string(content))
		if len(stmts) == 0 {
			log.Printf("[sqljob] skip empty sql file %q", f)
			continue
		}
		base := filepath.Base(f)
		if len(stmts) == 1 {
			units = append(units, unit{label: base, sql: stmts[0]})
			continue
		}
		for idx, s := range stmts {
			units = append(units, unit{label: fmt.Sprintf("%s#%d", base, idx+1), sql: s})
		}
	}

	return units, nil
}

func connLabel(db config.DBConfig) string {
	if name := strings.TrimSpace(db.Name); name != "" {
		return name
	}
	return strings.TrimSpace(db.ID)
}

func shortenSQL(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= 100 {
		return s
	}
	return s[:100] + "..."
}
