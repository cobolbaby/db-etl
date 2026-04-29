package config

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// JobDataSyncRow 对应 manager.job_data_sync 表中的一行记录。
type JobDataSyncRow struct {
	JobName            string
	SrcInstance        string // 引用 config.yaml 中 databases[].name
	SrcSchemaName      string
	SrcTableName       string
	SrcRawSQL          string // 优先使用；非空时作为 source.SQL
	SrcSelectStatement string // 次优先；SrcRawSQL 为空时使用
	SrcWhereStatement  string
	SrcIncrField       string
	SyncMode           string
	DstSchemaName      string
	DstTableName       string
	DstPK              string
}

// LoadTasksFromDB 连接 metaDB 所指定的 PostgreSQL 数据库，
// 查询 manager.job_data_sync 表中 job_name = jobName 且 inuse = true 的记录，
// 并将结果转换为 []TaskConfig 返回。
func LoadTasksFromDB(ctx context.Context, metaDB DBConfig, jobName string) ([]TaskConfig, error) {

	dsn := metaDB.DSN()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("connect meta db %q failed: %w", metaDB.Name, err)
	}
	defer conn.Close(ctx)

	const query = `
		SELECT
			job_name,
			COALESCE(src_instance, '')        AS src_instance,
			COALESCE(src_schema_name, '')     AS src_schema_name,
			COALESCE(src_table_name, '')      AS src_table_name,
			COALESCE(src_rawsql, '')          AS src_rawsql,
			COALESCE(src_select_statement, '') AS src_select_statement,
			COALESCE(src_where_statement, '') AS src_where_statement,
			COALESCE(src_incr_field, '')      AS src_incr_field,
			COALESCE(sync_mode, '')           AS sync_mode,
			COALESCE(dst_schema_name, '')     AS dst_schema_name,
			COALESCE(dst_table_name, '')      AS dst_table_name,
			COALESCE(dst_pk, '')              AS dst_pk
		FROM manager.job_data_sync
		WHERE job_name = $1
		  AND inuse = true
		ORDER BY job_id
	`

	rows, err := conn.Query(ctx, query, jobName)
	if err != nil {
		return nil, fmt.Errorf("query job_data_sync failed: %w", err)
	}
	defer rows.Close()

	var tasks []TaskConfig
	for rows.Next() {
		var r JobDataSyncRow
		if err := rows.Scan(
			&r.JobName,
			&r.SrcInstance,
			&r.SrcSchemaName,
			&r.SrcTableName,
			&r.SrcRawSQL,
			&r.SrcSelectStatement,
			&r.SrcWhereStatement,
			&r.SrcIncrField,
			&r.SyncMode,
			&r.DstSchemaName,
			&r.DstTableName,
			&r.DstPK,
		); err != nil {
			return nil, fmt.Errorf("scan job_data_sync row failed: %w", err)
		}

		task, err := rowToTaskConfig(r, metaDB.Name)
		if err != nil {
			return nil, fmt.Errorf("convert row to task config failed (job=%s src=%s.%s): %w",
				r.JobName, r.SrcSchemaName, r.SrcTableName, err)
		}
		tasks = append(tasks, task)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate job_data_sync rows failed: %w", err)
	}

	if len(tasks) == 0 {
		return nil, fmt.Errorf("no active tasks found for job_name=%q in manager.job_data_sync", jobName)
	}

	return tasks, nil
}

// rowToTaskConfig 将 job_data_sync 中的一行转换为 TaskConfig。
//
// SQL 来源优先级：
//  1. src_rawsql（非空时直接作为 source.SQL）
//  2. src_select_statement（非空时作为 source.SQL）
//  3. 否则使用 src_schema_name.src_table_name 作为 source.Table；
//     如果同时存在 src_where_statement，则拼装为完整 SQL。
func rowToTaskConfig(r JobDataSyncRow, defaultDstDB string) (TaskConfig, error) {

	if r.SrcInstance == "" {
		return TaskConfig{}, fmt.Errorf("src_instance is empty")
	}
	if r.DstSchemaName == "" || r.DstTableName == "" {
		return TaskConfig{}, fmt.Errorf("dst_schema_name / dst_table_name is empty")
	}

	src := &SourceConfig{
		DBName:    r.SrcInstance,
		BatchSize: 10000,
		IncrField: r.SrcIncrField,
	}

	switch {
	case strings.TrimSpace(r.SrcRawSQL) != "":
		src.SQL = r.SrcRawSQL

	case strings.TrimSpace(r.SrcSelectStatement) != "":
		src.SQL = r.SrcSelectStatement

	default:
		if r.SrcSchemaName == "" || r.SrcTableName == "" {
			return TaskConfig{}, fmt.Errorf("src_schema_name / src_table_name is empty and no sql provided")
		}
		tableRef := r.SrcSchemaName + "." + r.SrcTableName
		if strings.TrimSpace(r.SrcWhereStatement) != "" {
			// 拼装 SQL，保留 WHERE 条件
			src.SQL = fmt.Sprintf("SELECT * FROM %s WHERE %s", tableRef, r.SrcWhereStatement)
		} else {
			src.Table = tableRef
		}
	}

	target := &TargetConfig{
		// job_data_sync 表暂无 dst_instance 字段，默认使用 meta_db 作为目标库
		DBName: defaultDstDB,
		Table:  r.DstSchemaName + "." + r.DstTableName,
		Mode:   ModeType(r.SyncMode),
		PK:     r.DstPK,
	}

	return TaskConfig{
		Name:    r.JobName,
		Type:    TaskTypeEtl,
		Sources: []*SourceConfig{src},
		Target:  target,
	}, nil
}
