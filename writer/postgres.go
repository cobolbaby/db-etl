package writer

import (
	"bytes"
	"context"
	"db-etl/config"
	"db-etl/transform"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type PGWriter struct {
	*BaseWriter
}

type pgWriterDialect struct {
	conn *pgx.Conn
}

func NewPGWriter(conn *pgx.Conn, target *config.TargetConfig, jobName string) Writer {
	base := &BaseWriter{
		Target:  target,
		JobName: jobName,
	}

	base.dialect = &pgWriterDialect{conn: conn}

	return &PGWriter{BaseWriter: base}
}

func (d *pgWriterDialect) writeCopy(in <-chan transform.CSVBatch, table string) error {
	var firstBatch transform.CSVBatch
	foundRows := false
	for batch := range in {
		if len(batch.Rows) == 0 {
			continue
		}
		firstBatch = batch
		foundRows = true
		break
	}

	if !foundRows {
		log.Printf("table=%s no rows to copy, skip", table)
		return nil
	}

	return d.writeCopyWithFirstBatch(firstBatch, in, table)
}

func (d *pgWriterDialect) writeCopyWithFirstBatch(firstBatch transform.CSVBatch, in <-chan transform.CSVBatch, table string) error {
	return d.writeCopyStream(table, firstBatch.Columns, func(write func(transform.CSVBatch) error) error {
		if err := write(firstBatch); err != nil {
			return err
		}
		for batch := range in {
			if err := write(batch); err != nil {
				return err
			}
		}
		return nil
	})
}

// writeCopyStream 是 COPY 写入的底层实现。
// columns 用于构建 COPY SQL；iterFn 负责逐个传递所有 batch。
func (d *pgWriterDialect) writeCopyStream(
	table string,
	columns []string,
	iterFn func(write func(transform.CSVBatch) error) error,
) error {
	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	copySQL := buildCopySQL(table, columns)

	go func() {
		_, err := d.conn.PgConn().CopyFrom(context.Background(), pr, copySQL)
		if err != nil {
			_ = pr.CloseWithError(err)
		} else {
			_ = pr.Close()
		}
		errCh <- err
	}()

	buf := bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
	flushBuffer := func() error {
		if buf.Len() == 0 {
			return nil
		}

		if _, err := pw.Write(buf.Bytes()); err != nil {
			return err
		}

		buf.Reset()
		return nil
	}
	writeBatch := func(batch transform.CSVBatch) error {
		if len(batch.Rows) == 0 {
			return nil
		}

		for _, row := range batch.Rows {
			for i, col := range row {
				if i > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(col) // 简化版 CSV
			}
			buf.WriteByte('\n')
			if buf.Len() > 3*1024*1024 {
				if err := flushBuffer(); err != nil {
					return err
				}
			}
		}

		return nil
	}
	finishWithError := func(err error) error {
		_ = pw.CloseWithError(err)
		if copyErr := <-errCh; copyErr != nil {
			return copyErr
		}
		return err
	}
	finishCopy := func() error {
		if err := pw.Close(); err != nil {
			if copyErr := <-errCh; copyErr != nil {
				return copyErr
			}
			return err
		}

		log.Printf("COPY %s waiting for completion...", table)

		if err := <-errCh; err != nil {
			log.Printf("COPY %s error: %v", table, err)
			return err
		}

		log.Printf("COPY %s completed successfully", table)
		return nil
	}

	if err := iterFn(writeBatch); err != nil {
		return finishWithError(err)
	}

	if err := flushBuffer(); err != nil {
		return finishWithError(err)
	}

	return finishCopy()
}

func buildCopySQL(table string, columns []string) string {
	base := "COPY " + table
	if len(columns) > 0 {
		base += "(" + strings.Join(columns, ", ") + ")"
	}

	return base + " FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '')"
}

func (d *pgWriterDialect) writeMerge(in <-chan transform.CSVBatch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	if target.CommitBatchSize > 0 {
		return d.writeMergeSegmented(in, target, source, jobName)
	}
	return d.writeMergeOnce(in, target, source, jobName)
}

// writeMergeOnce 在单个事务中完成全量 merge（原有行为）。
func (d *pgWriterDialect) writeMergeOnce(in <-chan transform.CSVBatch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	var firstBatch transform.CSVBatch
	foundRows := false
	for batch := range in {
		if len(batch.Rows) == 0 {
			continue
		}
		firstBatch = batch
		foundRows = true
		break
	}

	if !foundRows {
		log.Printf("table=%s no incremental rows, skip merge", target.Table)
		return nil
	}

	ctx := context.Background()

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	staging := buildTempTableName(target.Table)

	if err := d.createTempTable(ctx, tx, staging, target); err != nil {
		return err
	}

	if err := d.writeCopyWithFirstBatch(firstBatch, in, staging); err != nil {
		return err
	}

	deleted, err := d.deleteTarget(ctx, tx, staging, target)
	if err != nil {
		return err
	}

	inserted, err := d.insertTarget(ctx, tx, staging, target)
	if err != nil {
		return err
	}

	if source != nil && source.IncrField != "" {
		maxWM, err := d.computeWatermark(ctx, tx, staging, source)
		if err != nil {
			return err
		}

		if err := d.updateWatermark(ctx, tx, maxWM, target, source, jobName); err != nil {
			return err
		}
	}

	log.Printf(
		"table=%s deleted=%d inserted=%d\n",
		target.Table, deleted, inserted,
	)

	return tx.Commit(ctx)
}

// writeMergeSegmented 将 merge 拆成若干段，每 CommitBatchSize 个 batch 提交一次事务并更新水位。
// 适用于超大表：中断后重启可从上次已提交的水位断点继续，而不必从头同步。
// batch 从 in 直接流入每段的 COPY goroutine，无需缓存到 slice。
func (d *pgWriterDialect) writeMergeSegmented(in <-chan transform.CSVBatch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	ctx := context.Background()
	var totalDeleted, totalInserted int64
	segmentIdx := 0

	// 当前段的状态
	var (
		currentTx      pgx.Tx
		currentStaging string
		feedCh         chan transform.CSVBatch
		copyDone       chan error
		batchCount     int
	)

	startSegment := func(columns []string) error {
		segmentIdx++
		tx, err := d.conn.Begin(ctx)
		if err != nil {
			return err
		}
		staging := buildTempTableName(target.Table)
		if err := d.createTempTable(ctx, tx, staging, target); err != nil {
			_ = tx.Rollback(ctx)
			return err
		}
		currentTx = tx
		currentStaging = staging
		feedCh = make(chan transform.CSVBatch, 1)
		copyDone = make(chan error, 1)
		go func() {
			copyDone <- d.writeCopyStream(staging, columns, func(write func(transform.CSVBatch) error) error {
				for b := range feedCh {
					if err := write(b); err != nil {
						return err
					}
				}
				return nil
			})
		}()
		return nil
	}

	commitSegment := func() error {
		close(feedCh)

		deleted, err := d.deleteTarget(ctx, currentTx, currentStaging, target)
		if err != nil {
			_ = currentTx.Rollback(ctx)
			return err
		}
		inserted, err := d.insertTarget(ctx, currentTx, currentStaging, target)
		if err != nil {
			_ = currentTx.Rollback(ctx)
			return err
		}
		totalDeleted += deleted
		totalInserted += inserted

		if source != nil && source.IncrField != "" {
			maxWM, err := d.computeWatermark(ctx, currentTx, currentStaging, source)
			if err != nil {
				_ = currentTx.Rollback(ctx)
				return err
			}
			if err := d.updateWatermark(ctx, currentTx, maxWM, target, source, jobName); err != nil {
				_ = currentTx.Rollback(ctx)
				return err
			}
			log.Printf("table=%s segment#%d: deleted=%d inserted=%d watermark=%s",
				target.Table, segmentIdx, deleted, inserted, maxWM)
		} else {
			log.Printf("table=%s segment#%d: deleted=%d inserted=%d",
				target.Table, segmentIdx, deleted, inserted)
		}
		return currentTx.Commit(ctx)
	}

	for batch := range in {
		if len(batch.Rows) == 0 {
			continue
		}
		if batchCount == 0 {
			if err := startSegment(batch.Columns); err != nil {
				return err
			}
		}

		// 同时监听 copyDone：若 copy goroutine 提前退出（如写入出错），
		// feedCh 将无人消费，直接阻塞在此会造成整条 pipeline 死锁。
		select {
		case feedCh <- batch:
			batchCount++
		case copyErr := <-copyDone:
			_ = currentTx.Rollback(ctx)
			if copyErr != nil {
				return fmt.Errorf("table=%s copy goroutine failed: %w", target.Table, copyErr)
			}
			return fmt.Errorf("table=%s copy goroutine exited unexpectedly", target.Table)
		}

		if batchCount >= target.CommitBatchSize {
			if err := commitSegment(); err != nil {
				return err
			}
			batchCount = 0
		}
	}

	if batchCount > 0 {
		if err := commitSegment(); err != nil {
			return err
		}
	}

	if segmentIdx == 0 {
		log.Printf("table=%s no incremental rows, skip merge", target.Table)
	} else {
		log.Printf("table=%s total: deleted=%d inserted=%d segments=%d",
			target.Table, totalDeleted, totalInserted, segmentIdx)
	}

	return nil
}

func (d *pgWriterDialect) createTempTable(ctx context.Context, tx pgx.Tx, staging string, target *config.TargetConfig) error {

	sql := fmt.Sprintf(
		`CREATE TEMP TABLE %s
		 (LIKE %s INCLUDING DEFAULTS)
		 ON COMMIT DROP`,
		staging, target.Table,
	)

	_, err := tx.Exec(ctx, sql)

	return err
}

func (d *pgWriterDialect) deleteTarget(
	ctx context.Context,
	tx pgx.Tx,
	staging string,
	target *config.TargetConfig,
) (int64, error) {

	sql := fmt.Sprintf(
		`DELETE FROM %s t USING %s s WHERE %s`,
		target.Table,
		staging,
		buildJoinCondition("t", "s", target.PK),
	)

	tag, err := tx.Exec(ctx, sql)
	if err != nil {
		return 0, err
	}

	return tag.RowsAffected(), nil
}

func (d *pgWriterDialect) insertTarget(ctx context.Context, tx pgx.Tx, staging string, target *config.TargetConfig) (int64, error) {

	sql := fmt.Sprintf(
		`INSERT INTO %s SELECT * FROM %s`,
		target.Table,
		staging,
	)

	tag, err := tx.Exec(ctx, sql)
	if err != nil {
		return 0, err
	}

	return tag.RowsAffected(), nil
}

func (d *pgWriterDialect) computeWatermark(ctx context.Context, tx pgx.Tx, staging string, source *config.SourceConfig) (string, error) {
	sql := fmt.Sprintf(
		`SELECT COALESCE(MAX(%s)::text, '') FROM %s`,
		source.IncrField,
		staging,
	)

	var wm string
	err := tx.QueryRow(ctx, sql).Scan(&wm)
	return wm, err
}

func (d *pgWriterDialect) updateWatermark(ctx context.Context, tx pgx.Tx, wm string, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	funcName := watermarkJobName(jobName)
	src, err := sourceIdentity(source)
	if err != nil {
		return err
	}
	dst, err := targetIdentity(target)
	if err != nil {
		return err
	}

	now := time.Now()

	var tag pgconn.CommandTag
	var execErr error
	if src.RawSQL != "" {
		tag, execErr = tx.Exec(
			ctx,
			`UPDATE manager.job_data_sync
			    SET incr_point      = $1,
			        sync_mode       = $2,
			        src_incr_field  = $3,
			        dst_pk          = $4,
			        udt             = $5
			  WHERE job_name        = $6
			    AND src_rawsql      = $7
			    AND dst_schema_name = $8
			    AND dst_table_name  = $9`,
			wm, string(target.Mode), source.IncrField, target.PK, now,
			funcName, src.RawSQL, dst.Schema, dst.Table,
		)
	} else {
		tag, execErr = tx.Exec(
			ctx,
			`UPDATE manager.job_data_sync
			    SET incr_point      = $1,
			        sync_mode       = $2,
			        src_incr_field  = $3,
			        dst_pk          = $4,
			        udt             = $5
			  WHERE job_name        = $6
			    AND src_schema_name = $7
			    AND src_table_name  = $8
			    AND dst_schema_name = $9
			    AND dst_table_name  = $10`,
			wm, string(target.Mode), source.IncrField, target.PK, now,
			funcName, src.Schema, src.Table, dst.Schema, dst.Table,
		)
	}
	if execErr != nil {
		return execErr
	}

	if tag.RowsAffected() > 0 {
		return nil
	}

	if src.RawSQL != "" {
		_, err = tx.Exec(
			ctx,
			`INSERT INTO manager.job_data_sync
			    (job_name, src_rawsql, dst_schema_name, dst_table_name,
			     incr_point, sync_mode, src_incr_field, dst_pk, cdt, udt)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $9)`,
			funcName, src.RawSQL, dst.Schema, dst.Table,
			wm, string(target.Mode), source.IncrField, target.PK, now,
		)
	} else {
		_, err = tx.Exec(
			ctx,
			`INSERT INTO manager.job_data_sync
			    (job_name, src_schema_name, src_table_name, dst_schema_name, dst_table_name,
			     incr_point, sync_mode, src_incr_field, dst_pk, cdt, udt)
			  VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)`,
			funcName, src.Schema, src.Table, dst.Schema, dst.Table,
			wm, string(target.Mode), source.IncrField, target.PK, now,
		)
	}
	return err
}

func (d *pgWriterDialect) getWatermark(target *config.TargetConfig, source *config.SourceConfig, jobName string) (string, error) {
	funcName := watermarkJobName(jobName)
	src, err := sourceIdentity(source)
	if err != nil {
		return "", err
	}
	dst, err := targetIdentity(target)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	var wm string
	if src.RawSQL != "" {
		err = d.conn.QueryRow(
			ctx,
			`SELECT COALESCE(incr_point, '')
			   FROM manager.job_data_sync
			  WHERE job_name = $1
			    AND src_rawsql = $2
			    AND dst_schema_name = $3
			    AND dst_table_name = $4
			  LIMIT 1`,
			funcName, src.RawSQL, dst.Schema, dst.Table,
		).Scan(&wm)
	} else {
		err = d.conn.QueryRow(
			ctx,
			`SELECT COALESCE(incr_point, '')
			   FROM manager.job_data_sync
			  WHERE job_name = $1
			    AND src_schema_name = $2
			    AND src_table_name = $3
			    AND dst_schema_name = $4
			    AND dst_table_name = $5
			  LIMIT 1`,
			funcName, src.Schema, src.Table, dst.Schema, dst.Table,
		).Scan(&wm)
	}

	if err != nil && err != pgx.ErrNoRows {
		return "", err
	}

	// 如果 job_data_sync 中没有记录或 incr_point 为空，回退到从目标表查增量字段最大值
	if wm == "" && source.IncrField != "" {
		ctx := context.Background()
		fallbackSQL := fmt.Sprintf(`SELECT COALESCE(MAX(%s)::text, '') FROM %s`, source.IncrField, target.Table)
		var fallback string
		if err := d.conn.QueryRow(ctx, fallbackSQL).Scan(&fallback); err != nil {
			return "", fmt.Errorf("fallback watermark query failed: %w", err)
		}
		if fallback != "" {
			log.Printf("watermark fallback: using MAX(%s)=%s from table %s", source.IncrField, fallback, target.Table)
			return fallback, nil
		}
		// 目标表也无数据，根据字段名推算兜底值
		defaultWM := defaultIncrPoint(source.IncrField)
		log.Printf("watermark fallback: no data in table %s, using default %s=%s", target.Table, source.IncrField, defaultWM)
		return defaultWM, nil
	}

	return wm, nil
}

// defaultIncrPoint 根据字段名推断兜底的增量起点：
// 包含 time/date/at/updated/created 等关键词时返回 "1970-01-01 00:00:00.000"，否则返回 "1"。
func defaultIncrPoint(incrField string) string {
	lower := strings.ToLower(incrField)
	timeKeywords := []string{"date", "time", "cdt", "udt", "create", "update", "modified"}
	for _, kw := range timeKeywords {
		if strings.Contains(lower, kw) {
			return "1970-01-01 00:00:00.000"
		}
	}
	return "1"
}

func splitQualifiedName(name string) (string, string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", "", fmt.Errorf("table name is required for watermark")
	}

	parts := strings.SplitN(trimmed, ".", 2)
	if len(parts) == 1 {
		return "", parts[0], nil
	}

	return parts[0], parts[1], nil
}

func buildTempTableName(fullTable string) string {
	schema := ""
	table := fullTable

	if strings.Contains(fullTable, ".") {
		parts := strings.SplitN(fullTable, ".", 2)
		schema = parts[0]
		table = parts[1]
	}

	prefix := table
	if schema != "" {
		prefix = schema + "_" + table
	}

	return fmt.Sprintf("%s_staging_%d", prefix, time.Now().UnixNano())
}

func buildJoinCondition(t1, t2 string, dstpk string) string {

	var parts []string

	cols := strings.Split(dstpk, ",")
	for _, c := range cols {
		parts = append(parts,
			fmt.Sprintf("%s.%s=%s.%s", t1, strings.Trim(c, " "), t2, strings.Trim(c, " ")))
	}

	return strings.Join(parts, " AND ")
}
