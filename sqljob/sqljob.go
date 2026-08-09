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
)

// stmtUnit 是一次“执行 + 重试”的最小单元（一条 SQL 语句），
// 并携带来源/责任人/备注等元信息，用于日志与失败定位。
type stmtUnit struct {
	source  string // 来源：SQL 文件名或 block 名称
	owner   string // 负责人（仅 block 提供）
	comment string // 备注（仅 block 提供）
	index   int    // 在来源内的序号（从 1 开始）
	sql     string
}

func (u stmtUnit) label() string {
	label := fmt.Sprintf("%s#%d", u.source, u.index)
	if u.owner != "" {
		label += " owner=" + u.owner
	}
	return label
}

// Run 执行一个脚本任务（type=sqljob）。
//
// 语句来源可以是 YAML 内联 blocks（带责任人/备注）或外部 SQL 文件，二者可混用；
// 都会被切分为独立语句，按顺序串行执行、每条独立提交（等价 psql 的 autocommit）。
//
// 重试的粒度是“单条语句”：某条语句失败仅重试该条，其前面已提交的语句不受影响；
// 该条语句最终仍失败时整个任务失败并终止。
func Run(ctx context.Context, dbCfg config.DBConfig, job config.SQLJobConfig, retryCfg util.RetryConfig) error {
	units, err := collectUnits(job)
	if err != nil {
		return err
	}
	if len(units) == 0 {
		return fmt.Errorf("no sql statements to execute")
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

	// 使用单个物理连接执行整个脚本，保持会话状态一致，
	// 同时让脚本内可能出现的显式 BEGIN/COMMIT 正确生效。
	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("acquire connection failed: %w", err)
	}
	defer conn.Close()

	for _, u := range units {
		u := u
		log.Printf("[sqljob] %s: exec %s %s", dbCfg.Name, u.label(), shortenSQL(u.sql))
		// 重试对象为单条语句，而非整个文件/任务。
		err := util.Retry(u.label(), retryCfg, func() error {
			_, execErr := conn.ExecContext(ctx, u.sql)
			return execErr
		})
		if err != nil {
			return fmt.Errorf("sqljob %s failed at %s: %w\n-- statement --\n%s",
				dbCfg.Name, u.label(), err, shortenSQL(u.sql))
		}
	}

	log.Printf("[sqljob] %s: completed %d statement(s)", dbCfg.Name, len(units))
	return nil
}

// collectUnits 汇总所有待执行语句：先内联 blocks，后 SQL 文件；均按声明顺序切分为独立语句。
func collectUnits(job config.SQLJobConfig) ([]stmtUnit, error) {
	var units []stmtUnit

	for _, b := range job.Blocks {
		name := strings.TrimSpace(b.Name)
		if name == "" {
			name = "block"
		}
		for i, s := range splitStatements(b.SQL) {
			units = append(units, stmtUnit{
				source:  name,
				owner:   strings.TrimSpace(b.Owner),
				comment: strings.TrimSpace(b.Comment),
				index:   i + 1,
				sql:     s,
			})
		}
	}

	files, err := resolveFiles(job.Files, len(job.Blocks) > 0)
	if err != nil {
		return nil, err
	}
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			return nil, fmt.Errorf("read file %s failed: %w", file, err)
		}
		for i, s := range splitStatements(string(data)) {
			units = append(units, stmtUnit{source: file, index: i + 1, sql: s})
		}
	}

	return units, nil
}

// resolveFiles 归一化待执行的文件列表。
// 显式给定则原样使用（相对路径基于运行目录）；
// 为空且未配置 blocks 时回退到运行目录下的所有 *.sql；已配置 blocks 时不再扫描目录。
func resolveFiles(files []string, hasBlocks bool) ([]string, error) {
	var out []string
	for _, f := range files {
		if strings.TrimSpace(f) != "" {
			out = append(out, f)
		}
	}
	if len(out) > 0 {
		return out, nil
	}
	if hasBlocks {
		return nil, nil
	}

	matches, err := filepath.Glob("*.sql")
	if err != nil {
		return nil, fmt.Errorf("glob *.sql failed: %w", err)
	}
	sort.Strings(matches)
	return matches, nil
}

func shortenSQL(sql string) string {
	sql = strings.Join(strings.Fields(sql), " ")
	if len(sql) <= 120 {
		return sql
	}
	return sql[:120] + "..."
}
