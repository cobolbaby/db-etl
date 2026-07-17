package config

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// JobDataSyncRow 对应 manager.job_data_sync 表中的一行记录。
type JobDataSyncRow struct {
	JobName           string
	SrcConnID         string // 引用 config.yaml 中 databases[].id
	SrcDBName         string
	SrcSchemaName     string
	SrcTableName      string
	SrcRawSQL         string // 优先使用；非空时作为 source.SQL
	SrcWhereStatement string
	FieldsMapping     string
	SrcIncrField      string
	SrcIncrPoint      string
	SyncMode          string
	DstSchemaName     string
	DstTableName      string
	DstPK             string
}

// LoadTasksFromDB 连接 metaDB 所指定的 PostgreSQL 数据库，
// 查询 manager.job_data_sync 表中 job_name = jobName 且 inuse = true 的记录，
// 并将结果转换为 []TaskConfig 返回。
func LoadTasksFromDB(ctx context.Context, metaDB DBConfig, jobName string, resolver DBResolver) ([]TaskConfig, error) {

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
			COALESCE(src_db_name, '')         AS src_db_name,
			COALESCE(src_schema_name, '')     AS src_schema_name,
			COALESCE(src_table_name, '')      AS src_table_name,
			COALESCE(src_rawsql, '')          AS src_rawsql,
			COALESCE(src_where_statement, '') AS src_where_statement,
			COALESCE(fields_mapping::text, '') AS fields_mapping,
			COALESCE(src_incr_field, '')      AS src_incr_field,
			COALESCE(incr_point, '')          AS incr_point,
			COALESCE(sync_mode, '')           AS sync_mode,
			COALESCE(dst_schema_name, '')     AS dst_schema_name,
			COALESCE(dst_table_name, '')      AS dst_table_name,
			COALESCE(dst_pk, '')              AS dst_pk
		FROM manager.job_data_sync
		WHERE job_name = $1
		  AND inuse = true
		  AND sync_mode IN ('full', 'append', 'merge')
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
			&r.SrcDBName,
			&r.SrcSchemaName,
			&r.SrcTableName,
			&r.SrcRawSQL,
			&r.SrcWhereStatement,
			&r.FieldsMapping,
			&r.SrcIncrField,
			&r.SrcIncrPoint,
			&r.SyncMode,
			&r.DstSchemaName,
			&r.DstTableName,
			&r.DstPK,
		); err != nil {
			return nil, fmt.Errorf("scan job_data_sync row failed: %w", err)
		}

		task, err := rowToTaskConfig(r, metaDB, resolver)
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
//  2. 否则使用 src_schema_name.src_table_name 作为 source.Table（MSSQL 源时在前面加 src_db_name 前缀）；
//     并将 src_where_statement / fields_mapping
//     写入 source.WhereStatement / source.FieldsMapping，留到 reader 阶段再拼装查询。
func rowToTaskConfig(r JobDataSyncRow, metaDB DBConfig, resolver DBResolver) (TaskConfig, error) {

	if r.SrcConnID == "" {
		return TaskConfig{}, fmt.Errorf("src_conn_id is empty")
	}
	if r.DstSchemaName == "" || r.DstTableName == "" {
		return TaskConfig{}, fmt.Errorf("dst_schema_name / dst_table_name is empty")
	}
	// src_db_name 作为 watermark 的 src_db_name，正常不应为空；为空会导致水位定位不准确。
	if strings.TrimSpace(r.SrcDBName) == "" {
		return TaskConfig{}, fmt.Errorf("src_db_name is empty")
	}

	src := &SourceConfig{
		// 通过 src_conn_id 匹配数据源。
		ConnID: r.SrcConnID,
		// Database 直接取自 job_data_sync 的 src_db_name，作为 watermark 的 src_db_name。
		// 它反映真实的源库（可能与数据源连接默认库不同），后续流程不得用连接配置的库名覆盖。
		Database:       strings.TrimSpace(r.SrcDBName),
		WhereStatement: strings.TrimSpace(r.SrcWhereStatement),
		BatchSize:      10000,
		Mode:           ModeType(r.SyncMode),
		IncrField:      r.SrcIncrField,
		IncrPoint:      strings.TrimSpace(r.SrcIncrPoint),
	}

	fieldsMapping := strings.TrimSpace(r.FieldsMapping)
	if fieldsMapping != "" && fieldsMapping != "*" {
		parsedMapping, err := ParseFieldsMapping(fieldsMapping)
		if err != nil {
			return TaskConfig{}, fmt.Errorf("parse fields_mapping failed: %w", err)
		}
		src.FieldsMapping = parsedMapping
	}

	switch {
	case strings.TrimSpace(r.SrcRawSQL) != "":
		src.SQL = r.SrcRawSQL

	default:
		if r.SrcSchemaName == "" || r.SrcTableName == "" {
			return TaskConfig{}, fmt.Errorf("src_schema_name / src_table_name is empty and no sql provided")
		}
		srcDBName := strings.TrimSpace(r.SrcDBName)
		srcDB, ok := resolver.Resolve(r.SrcConnID, "")
		if srcDBName != "" && ok && srcDB.Type == DBTypeMSSQL {
			src.Table = srcDBName + "." + r.SrcSchemaName + "." + r.SrcTableName
		} else {
			src.Table = r.SrcSchemaName + "." + r.SrcTableName
		}
	}

	target := &TargetConfig{
		// job_data_sync 表暂无 dst_instance 字段，默认使用 meta_db 作为目标库
		ConnID:   metaDB.ID,
		ConnName: metaDB.Name,
		Table:    r.DstSchemaName + "." + r.DstTableName,
		Mode:     ModeType(r.SyncMode),
		PK:       r.DstPK,
	}

	return TaskConfig{
		Name:    r.JobName,
		Type:    TaskTypeEtl,
		Sources: []*SourceConfig{src},
		Target:  target,
	}, nil
}
