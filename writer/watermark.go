package writer

import (
	"context"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

func watermarkJobName(jobName string) string {
	return strings.TrimSpace(jobName)
}

// tableRef 是 watermark 里「表标识」的统一表示：
// 目标端只用到 Schema/Table（Database 恒为空，由 targetIdentity 强制），
// 源端则可能带上库名（MSSQL 的 db.schema.table）。
type tableRef struct {
	Database string
	Schema   string
	Table    string
}

// wmSource 在表标识之上多一个 RawSQL 分支：
// 源可以是一张表，也可以是一段自定义 SQL，两者互斥。
// ConnID/ConnName 是源连接标识（已归一化：空串转为 nil，供 pgx 写入 SQL NULL），
// 用于区分「同名源库/表经不同连接同步到同一目标」的场景。
type wmSource struct {
	tableRef
	RawSQL   string
	ConnID   any
	ConnName any
}

func sourceIdentity(source *config.SourceConfig) (wmSource, error) {
	if source == nil {
		return wmSource{}, fmt.Errorf("source config is required for watermark")
	}

	// 源连接标识对表/SQL 两种分支都适用，统一归一化后回填。
	connID := nullIfEmpty(source.ConnID)
	connName := nullIfEmpty(source.ConnName)

	if table := strings.TrimSpace(source.Table); table != "" {
		parts, err := splitTableRef(table)
		if err != nil {
			return wmSource{}, err
		}
		// 三段式表名仅 MSSQL 合法，已在加载阶段由 config.ValidateTableName 保证；
		// 此处只做归属推导：以表名内的库名为准，否则回退到数据源连接的库名，
		// 确保 watermark 的 src_db_name 始终反映真实的源库。
		if parts.Database == "" {
			parts.Database = strings.TrimSpace(source.Database)
		}
		return wmSource{tableRef: parts, ConnID: connID, ConnName: connName}, nil
	}

	if sql := strings.TrimSpace(source.SQL); sql != "" {
		return wmSource{
			tableRef: tableRef{Database: strings.TrimSpace(source.Database)},
			RawSQL:   sql,
			ConnID:   connID,
			ConnName: connName,
		}, nil
	}

	return wmSource{}, fmt.Errorf("source identity is required for watermark")
}

func targetIdentity(target *config.TargetConfig) (tableRef, error) {
	if target == nil {
		return tableRef{}, fmt.Errorf("target config is required for watermark")
	}

	parts, err := splitTableRef(target.Table)
	if err != nil {
		return tableRef{}, err
	}
	if parts.Database != "" {
		return tableRef{}, fmt.Errorf("target table must be table or schema.table")
	}

	return parts, nil
}

func splitTableRef(name string) (tableRef, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return tableRef{}, fmt.Errorf("table name is required for watermark")
	}

	parts := strings.Split(trimmed, ".")
	switch len(parts) {
	case 1:
		return tableRef{Table: parts[0]}, nil
	case 2:
		return tableRef{Schema: parts[0], Table: parts[1]}, nil
	case 3:
		return tableRef{Database: parts[0], Schema: parts[1], Table: parts[2]}, nil
	default:
		return tableRef{}, fmt.Errorf("table name must be table, schema.table, or db.schema.table")
	}
}

// pgxExecutor 统一 pgx.Tx 与 *pgx.Conn 两个具体类型的执行方法。
// 它不是「跨数据库」抽象：manager.job_data_sync 固定在 PostgreSQL，底层始终是 pgx。
// 定义它仅因为 pgx 未导出涵盖 Exec/QueryRow 的公共接口，而水位读写需要同时支持两种场景：
// PG writer 在事务（pgx.Tx）内随数据原子提交，parquet writer 则走独立连接（*pgx.Conn）。
type pgxExecutor interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// upsertWatermark 将水位写回 manager.job_data_sync：按 (job_name + 源标识 + 目标标识) 定位记录，
// 命中则 UPDATE，否则 INSERT。ex 可传入 pgx.Tx（PG writer，随事务原子提交）或 *pgx.Conn（parquet writer）。
func upsertWatermark(ctx context.Context, ex pgxExecutor, wm string, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	funcName := watermarkJobName(jobName)
	src, err := sourceIdentity(source)
	if err != nil {
		return err
	}
	dst, err := targetIdentity(target)
	if err != nil {
		return err
	}

	// 源连接标识参与匹配键，用于区分「同名源库/表经不同连接同步到同一目标」的场景。
	// 但因历史原因，conn_id / conn_name 通常只有其一非空（另一个可能为 NULL），
	// 故只约束本次运行实际携带的那个标识：($p IS NULL OR col = $p)——
	// 参数为空则不约束该列，避免历史空值导致漏配、进而重复 INSERT 或水位重置。
	var tag pgconn.CommandTag
	var execErr error
	if src.RawSQL != "" {
		tag, execErr = ex.Exec(
			ctx,
			`UPDATE manager.job_data_sync
			    SET incr_point      = $1,
			        sync_mode       = $2,
			        src_incr_field  = $3,
			        dst_pk          = $4,
			        udt             = now()
			  WHERE job_name        = $5
			    AND src_db_name     = $6
			    AND src_rawsql      = $7
			    AND dst_schema_name = $8
			    AND dst_table_name  = $9
			    AND ($10::int IS NULL OR src_conn_id = $10::int)
			    AND ($11::text IS NULL OR src_conn_name = $11::text)`,
			wm, string(target.Mode), source.IncrField, target.PK,
			funcName, src.Database, src.RawSQL, dst.Schema, dst.Table,
			src.ConnID, src.ConnName,
		)
	} else {
		tag, execErr = ex.Exec(
			ctx,
			`UPDATE manager.job_data_sync
			    SET incr_point      = $1,
			        sync_mode       = $2,
			        src_incr_field  = $3,
			        dst_pk          = $4,
			        udt             = now()
			  WHERE job_name        = $5
			    AND src_db_name     = $6
			    AND src_schema_name = $7
			    AND src_table_name  = $8
			    AND dst_schema_name = $9
			    AND dst_table_name  = $10
			    AND ($11::int IS NULL OR src_conn_id = $11::int)
			    AND ($12::text IS NULL OR src_conn_name = $12::text)`,
			wm, string(target.Mode), source.IncrField, target.PK,
			funcName, src.Database, src.Schema, src.Table, dst.Schema, dst.Table,
			src.ConnID, src.ConnName,
		)
	}
	if execErr != nil {
		return util.WrapPgError(execErr)
	}

	if tag.RowsAffected() > 0 {
		return nil
	}

	// 新建水位行时写入源连接标识：src_conn_id 与 src_conn_name 至少有一个非空
	// （由 manager.job_data_sync 的 CHECK 约束 chk_src_conn_not_both_null 保证）。
	// ConnID / ConnName 已由 sourceIdentity 归一化：空串转为 nil，pgx 写入 SQL NULL。
	if src.RawSQL != "" {
		_, err = ex.Exec(
			ctx,
			`INSERT INTO manager.job_data_sync
			    (job_name, src_db_name, src_conn_id, src_conn_name, src_rawsql, dst_schema_name, dst_table_name,
			     incr_point, sync_mode, src_incr_field, dst_pk, cdt, udt)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now(), now())`,
			funcName, src.Database, src.ConnID, src.ConnName, src.RawSQL, dst.Schema, dst.Table,
			wm, string(target.Mode), source.IncrField, target.PK,
		)
	} else {
		_, err = ex.Exec(
			ctx,
			`INSERT INTO manager.job_data_sync
			    (job_name, src_db_name, src_conn_id, src_conn_name, src_schema_name, src_table_name, dst_schema_name, dst_table_name,
			     incr_point, sync_mode, src_incr_field, dst_pk, cdt, udt)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, now(), now())`,
			funcName, src.Database, src.ConnID, src.ConnName, src.Schema, src.Table, dst.Schema, dst.Table,
			wm, string(target.Mode), source.IncrField, target.PK,
		)
	}
	return util.WrapPgError(err)
}

// SyncStatus 是 manager.job_data_sync.last_status 的取值：
//   - SyncStatusSuccess(0)：最近一次同步成功。
//   - SyncStatusFailed(1)：最近一次同步失败（当前统一预设为 1，后续可细化为不同错误码）。
const (
	SyncStatusSuccess = 0
	SyncStatusFailed  = 1
)

// execUpdateLastStatus 把最近一次同步结果写回 manager.job_data_sync.last_status：
// 按 (job_name + 源标识 + 目标标识) 定位记录（与 upsertWatermark 一致），命中则更新并返回受影响行数。
// ex 可传入 pgx.Tx（PG writer）或 *pgx.Conn（parquet writer）。
func execUpdateLastStatus(ctx context.Context, ex pgxExecutor, status int, target *config.TargetConfig, source *config.SourceConfig, jobName string) (int64, error) {
	funcName := watermarkJobName(jobName)
	src, err := sourceIdentity(source)
	if err != nil {
		return 0, err
	}
	dst, err := targetIdentity(target)
	if err != nil {
		return 0, err
	}

	// 与 upsertWatermark 保持一致：仅约束本次运行实际携带的连接标识（参数为空则不约束该列）。
	var tag pgconn.CommandTag
	if src.RawSQL != "" {
		tag, err = ex.Exec(
			ctx,
			`UPDATE manager.job_data_sync
			    SET last_status = $1,
			        udt         = now()
			  WHERE job_name        = $2
			    AND src_db_name     = $3
			    AND src_rawsql      = $4
			    AND dst_schema_name = $5
			    AND dst_table_name  = $6
			    AND ($7::int IS NULL OR src_conn_id = $7::int)
			    AND ($8::text IS NULL OR src_conn_name = $8::text)`,
			status, funcName, src.Database, src.RawSQL, dst.Schema, dst.Table,
			src.ConnID, src.ConnName,
		)
	} else {
		tag, err = ex.Exec(
			ctx,
			`UPDATE manager.job_data_sync
			    SET last_status = $1,
			        udt         = now()
			  WHERE job_name        = $2
			    AND src_db_name     = $3
			    AND src_schema_name = $4
			    AND src_table_name  = $5
			    AND dst_schema_name = $6
			    AND dst_table_name  = $7
			    AND ($8::int IS NULL OR src_conn_id = $8::int)
			    AND ($9::text IS NULL OR src_conn_name = $9::text)`,
			status, funcName, src.Database, src.Schema, src.Table, dst.Schema, dst.Table,
			src.ConnID, src.ConnName,
		)
	}
	if err != nil {
		return 0, util.WrapPgError(err)
	}
	return tag.RowsAffected(), nil
}

// nullIfEmpty 将空字符串归一为 nil，使 pgx 写入 SQL NULL 而非空串，
// 以满足 src_conn_name 与 src_conn_id 的“不能同时非空串/非空”约束语义。
func nullIfEmpty(s string) any {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return s
}

// readWatermarkPoint 从 manager.job_data_sync 读取已记录的 incr_point。
// 未命中记录时返回空串且 err 为 nil（由调用方决定兜底策略）。
func readWatermarkPoint(ctx context.Context, ex pgxExecutor, target *config.TargetConfig, source *config.SourceConfig, jobName string) (string, error) {
	funcName := watermarkJobName(jobName)
	src, err := sourceIdentity(source)
	if err != nil {
		return "", err
	}
	dst, err := targetIdentity(target)
	if err != nil {
		return "", err
	}

	// 与 upsertWatermark 保持一致：仅约束本次运行实际携带的连接标识（参数为空则不约束该列）。
	var wm string
	if src.RawSQL != "" {
		err = ex.QueryRow(
			ctx,
			`SELECT COALESCE(incr_point, '')
			   FROM manager.job_data_sync
			  WHERE job_name = $1
			    AND src_db_name = $2
			    AND src_rawsql = $3
			    AND dst_schema_name = $4
			    AND dst_table_name = $5
			    AND ($6::int IS NULL OR src_conn_id = $6::int)
			    AND ($7::text IS NULL OR src_conn_name = $7::text)
			  LIMIT 1`,
			funcName, src.Database, src.RawSQL, dst.Schema, dst.Table,
			src.ConnID, src.ConnName,
		).Scan(&wm)
	} else {
		err = ex.QueryRow(
			ctx,
			`SELECT COALESCE(incr_point, '')
			   FROM manager.job_data_sync
			  WHERE job_name = $1
			    AND src_db_name = $2
			    AND src_schema_name = $3
			    AND src_table_name = $4
			    AND dst_schema_name = $5
			    AND dst_table_name = $6
			    AND ($7::int IS NULL OR src_conn_id = $7::int)
			    AND ($8::text IS NULL OR src_conn_name = $8::text)
			  LIMIT 1`,
			funcName, src.Database, src.Schema, src.Table, dst.Schema, dst.Table,
			src.ConnID, src.ConnName,
		).Scan(&wm)
	}

	if err != nil && err != pgx.ErrNoRows {
		return "", err
	}
	return wm, nil
}

// execDeactivateInitialJob 将 manager.job_data_sync 中匹配记录置为 inuse=false，返回受影响行数。
// initial（首次全量）成功后调用，避免下次重复回填。ex 可为事务或独立连接。
func execDeactivateInitialJob(ctx context.Context, ex pgxExecutor, target *config.TargetConfig, source *config.SourceConfig, jobName string) (int64, error) {
	funcName := watermarkJobName(jobName)
	src, err := sourceIdentity(source)
	if err != nil {
		return 0, err
	}
	dst, err := targetIdentity(target)
	if err != nil {
		return 0, err
	}

	// 与 upsertWatermark 保持一致：仅约束本次运行实际携带的连接标识（参数为空则不约束该列）。
	var tag pgconn.CommandTag
	if src.RawSQL != "" {
		tag, err = ex.Exec(
			ctx,
			`UPDATE manager.job_data_sync
			    SET inuse = false,
			        udt   = now()
			  WHERE job_name        = $1
			    AND src_db_name     = $2
			    AND src_rawsql      = $3
			    AND dst_schema_name = $4
			    AND dst_table_name  = $5
			    AND ($6::int IS NULL OR src_conn_id = $6::int)
			    AND ($7::text IS NULL OR src_conn_name = $7::text)`,
			funcName, src.Database, src.RawSQL, dst.Schema, dst.Table,
			src.ConnID, src.ConnName,
		)
	} else {
		tag, err = ex.Exec(
			ctx,
			`UPDATE manager.job_data_sync
			    SET inuse = false,
			        udt   = now()
			  WHERE job_name        = $1
			    AND src_db_name     = $2
			    AND src_schema_name = $3
			    AND src_table_name  = $4
			    AND dst_schema_name = $5
			    AND dst_table_name  = $6
			    AND ($7::int IS NULL OR src_conn_id = $7::int)
			    AND ($8::text IS NULL OR src_conn_name = $8::text)`,
			funcName, src.Database, src.Schema, src.Table, dst.Schema, dst.Table,
			src.ConnID, src.ConnName,
		)
	}
	if err != nil {
		return 0, util.WrapPgError(err)
	}
	return tag.RowsAffected(), nil
}
