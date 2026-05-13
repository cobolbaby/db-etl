package config

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// JobDataSyncRow 对应 manager.job_data_sync_v2 表中的一行记录。
type JobDataSyncRow struct {
	JobName            string
	SrcConnID          string // 引用 config.yaml 中 databases[].id
	SrcConnName        string // 通过 SrcConnID 在 config.yaml 中找到对应的数据库配置
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
// 查询 manager.job_data_sync_v2 表中 job_name = jobName 且 inuse = true 的记录，
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
			src_conn_id,
			COALESCE(src_instance, '')       AS src_conn_name,
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
		FROM manager.job_data_sync_v2
		WHERE job_name = $1
		  AND inuse = true
		  AND (sync_mode IN ('full', 'append', 'merge')
		  	or (sync_mode = 'cdc' AND dst_pk <> ''))
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
			&r.SrcConnID,
			&r.SrcConnName,
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
		return nil, fmt.Errorf("no active tasks found for job_name=%q in manager.job_data_sync_v2", jobName)
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

	if r.SrcConnName == "" && r.SrcConnID == "" {
		return TaskConfig{}, fmt.Errorf("src_instance and src_conn_id are empty")
	}
	if r.DstSchemaName == "" || r.DstTableName == "" {
		return TaskConfig{}, fmt.Errorf("dst_schema_name / dst_table_name is empty")
	}

	// TODO: 访问 DTS HTTP 接口，传入 SrcConnID，获取对应的数据库连接配置（DBConfig），并将 DBConfig.Name 赋值给 SrcConnName。

	src := &SourceConfig{
		DBName:    r.SrcConnName,
		BatchSize: 10000,
		IncrField: r.SrcIncrField,
	}

	switch {
	case strings.TrimSpace(r.SrcRawSQL) != "":
		src.SQL = r.SrcRawSQL

	default:
		if r.SrcSchemaName == "" || r.SrcTableName == "" {
			return TaskConfig{}, fmt.Errorf("src_schema_name / src_table_name is empty and no sql provided")
		}
		tableRef := r.SrcSchemaName + "." + r.SrcTableName
		selectClause := strings.TrimSpace(r.SrcSelectStatement)
		if selectClause == "" {
			selectClause = "*"
		}
		whereClause := strings.TrimSpace(r.SrcWhereStatement)
		if whereClause != "" {
			src.SQL = fmt.Sprintf("SELECT %s FROM %s WHERE %s", selectClause, tableRef, whereClause)
		} else if selectClause != "*" {
			// 有自定义列但无 WHERE 条件
			src.SQL = fmt.Sprintf("SELECT %s FROM %s", selectClause, tableRef)
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
