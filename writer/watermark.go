package writer

import (
	"context"
	"db-etl/config"
	"db-etl/util"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

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
			    AND dst_table_name  = $9`,
			wm, string(target.Mode), source.IncrField, target.PK,
			funcName, src.Database, src.RawSQL, dst.Schema, dst.Table,
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
			    AND dst_table_name  = $10`,
			wm, string(target.Mode), source.IncrField, target.PK,
			funcName, src.Database, src.Schema, src.Table, dst.Schema, dst.Table,
		)
	}
	if execErr != nil {
		return util.WrapPgError(execErr)
	}

	if tag.RowsAffected() > 0 {
		return nil
	}

	if src.RawSQL != "" {
		_, err = ex.Exec(
			ctx,
			`INSERT INTO manager.job_data_sync
			    (job_name, src_db_name, src_rawsql, dst_schema_name, dst_table_name,
			     incr_point, sync_mode, src_incr_field, dst_pk, cdt, udt)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, now(), now())`,
			funcName, src.Database, src.RawSQL, dst.Schema, dst.Table,
			wm, string(target.Mode), source.IncrField, target.PK,
		)
	} else {
		_, err = ex.Exec(
			ctx,
			`INSERT INTO manager.job_data_sync
			    (job_name, src_db_name, src_schema_name, src_table_name, dst_schema_name, dst_table_name,
			     incr_point, sync_mode, src_incr_field, dst_pk, cdt, udt)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, now(), now())`,
			funcName, src.Database, src.Schema, src.Table, dst.Schema, dst.Table,
			wm, string(target.Mode), source.IncrField, target.PK,
		)
	}
	return util.WrapPgError(err)
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
			  LIMIT 1`,
			funcName, src.Database, src.RawSQL, dst.Schema, dst.Table,
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
			  LIMIT 1`,
			funcName, src.Database, src.Schema, src.Table, dst.Schema, dst.Table,
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
			    AND dst_table_name  = $5`,
			funcName, src.Database, src.RawSQL, dst.Schema, dst.Table,
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
			    AND dst_table_name  = $6`,
			funcName, src.Database, src.Schema, src.Table, dst.Schema, dst.Table,
		)
	}
	if err != nil {
		return 0, util.WrapPgError(err)
	}
	return tag.RowsAffected(), nil
}
