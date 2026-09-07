package writer

import (
	"bytes"
	"context"
	"db-etl/config"
	"db-etl/reader"
	"db-etl/util"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type pgWriterDialect struct {
	conn *pgx.Conn
}

func NewPGWriter(conn *pgx.Conn, target *config.TargetConfig, jobName string) Writer {
	base := &BaseWriter{
		Target:  target,
		JobName: jobName,
	}

	base.dialect = &pgWriterDialect{conn: conn}

	return base
}

// close 关闭 pgx 连接。
func (d *pgWriterDialect) close(ctx context.Context) error {
	if d.conn == nil {
		return nil
	}
	return d.conn.Close(ctx)
}

// drainFirstBatch 从 channel 中取出第一个非空 batch，用于获取列信息并启动后续写入。
func drainFirstBatch(in <-chan reader.Batch) (reader.Batch, bool) {
	for batch := range in {
		if len(batch.Rows) == 0 {
			continue
		}
		return batch, true
	}
	return reader.Batch{}, false
}

func (d *pgWriterDialect) writeFull(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig) error {

	firstBatch, foundRows := drainFirstBatch(in)

	// 源端为空同样是一种有效状态（数据被清空/条件过滤后无结果），仍需清表提交，
	// 否则目标表会残留上一轮的过期数据。
	// if !foundRows {
	// 	log.Printf("table=%s full refresh finished: no rows to load", target.Table)
	// 	return nil
	// }

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// 尝试带超时的 TRUNCATE；若锁等待超时则回滚当前事务，开新事务用 DELETE FROM 退避
	if err := d.tryTruncateWithTimeout(ctx, tx, target); err != nil {
		if !util.IsPgLockTimeout(err) {
			return util.WrapPgError(err)
		}

		log.Printf("table=%s TRUNCATE lock timeout, falling back to DELETE FROM", target.Table)

		_ = tx.Rollback(ctx)

		tx, err = d.conn.Begin(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)

		if _, err := tx.Exec(ctx, fmt.Sprintf("DELETE FROM %s", target.Table)); err != nil {
			return util.WrapPgError(err)
		}
		log.Printf("table=%s DELETE FROM completed", target.Table)
	}

	// 使用事务所在连接执行 COPY，确保 COPY 与清表操作在同一事务内原子提交。
	if foundRows {
		if err := d.writeCopyWithFirstBatch(ctx, firstBatch, in, target.Table, tx.Conn()); err != nil {
			return err
		}
		log.Printf("table=%s full refresh finished: target replaced", target.Table)
	} else {
		log.Printf("table=%s full refresh finished: source has no rows, target cleared", target.Table)
	}

	return tx.Commit(ctx)
}

func (d *pgWriterDialect) writeInitial(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	firstBatch, foundRows := drainFirstBatch(in)

	// COPY 与「任务下线」放在同一事务内提交，保证 initial（首次全量）回填成功后
	// job_data_sync 才被置为 inuse=false，避免下次重复回填上亿行。
	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if foundRows {
		if err := d.writeCopyWithFirstBatch(ctx, firstBatch, in, target.Table, tx.Conn()); err != nil {
			return err
		}
	} else {
		log.Printf("table=%s no rows to copy, skip", target.Table)
	}

	if err := d.deactivateInitialJob(ctx, tx, target, source, jobName); err != nil {
		return err
	}

	return tx.Commit(ctx)
}

// deactivateInitialJob 在 initial 模式成功后，将 manager.job_data_sync 中对应记录置为
// inuse=false，避免下次运行重复执行首次全量回填。
//
// 与 updateWatermark 一致，按 (job_name + 源标识 + 目标标识) 定位记录，无需外层透传 job_id。
// 若没有匹配到记录（如纯 config.yaml 任务），RowsAffected 为 0，视为无操作。
func (d *pgWriterDialect) deactivateInitialJob(ctx context.Context, tx pgx.Tx, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	affected, err := execDeactivateInitialJob(ctx, tx, target, source, jobName)
	if err != nil {
		return err
	}
	funcName := watermarkJobName(jobName)
	if affected == 0 {
		log.Printf("initial task done but no matching job_data_sync row (job=%s table=%s), inuse unchanged", funcName, target.Table)
		return nil
	}
	log.Printf("initial task done, set inuse=false (job=%s table=%s)", funcName, target.Table)
	return nil
}

func (d *pgWriterDialect) writeCopyWithFirstBatch(ctx context.Context, firstBatch reader.Batch, in <-chan reader.Batch, table string, conn *pgx.Conn) error {
	return d.writeCopyStream(ctx, table, firstBatch.ColumnNames(), conn, func(write func(reader.Batch) error) error {
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
// columns 用于构建 COPY SQL；conn 为执行 COPY 的连接（可传入 tx.Conn() 以确保在同一事务内）；
// iterFn 负责逐个传递所有 batch。
func (d *pgWriterDialect) writeCopyStream(
	ctx context.Context,
	table string,
	columns []string,
	conn *pgx.Conn,
	iterFn func(write func(reader.Batch) error) error,
) error {
	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	copySQL := buildCopySQL(table, columns)

	var copyTag pgconn.CommandTag
	go func() {
		tag, err := conn.PgConn().CopyFrom(ctx, pr, copySQL)
		copyTag = tag
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
	// CSV 文本编码在此处（而非 reader/transform）完成：它是 COPY 的输入格式要求，
	// 与目标绑定；parquet 目标则直接消费原始值，无需经过字符串中转。
	writeBatch := func(batch reader.Batch) error {
		if len(batch.Rows) == 0 {
			return nil
		}

		for _, row := range batch.Rows {
			for i, v := range row {
				if i > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(encodeCopyValue(batch.Columns[i].Kind, v))
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
			return util.WrapPgError(copyErr)
		}
		return util.WrapPgError(err)
	}

	if err := iterFn(writeBatch); err != nil {
		return finishWithError(err)
	}

	if err := flushBuffer(); err != nil {
		return finishWithError(err)
	}

	pw.Close()
	if err := <-errCh; err != nil {
		log.Printf("COPY %s error: %v", table, err)
		return util.WrapPgError(err)
	}
	log.Printf("COPY %s completed successfully, rows=%d", table, copyTag.RowsAffected())
	return nil
}

// buildCopySQL 以 nullSentinel 作为 NULL 标记，令 COPY 能区分 NULL 与空字符串。
func buildCopySQL(table string, columns []string) string {
	base := "COPY " + table
	if len(columns) > 0 {
		base += "(" + strings.Join(columns, ", ") + ")"
	}

	return base + " FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '" + nullSentinel + "')"
}

func (d *pgWriterDialect) writeAppend(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	if target.CommitBatchSize > 0 {
		return d.writeIncrChunked(ctx, in, target, source, jobName, false)
	}
	return d.writeIncrOnce(ctx, in, target, source, jobName, false)
}

func (d *pgWriterDialect) writeMerge(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	if target.CommitBatchSize > 0 {
		return d.writeIncrChunked(ctx, in, target, source, jobName, true)
	}
	return d.writeIncrOnce(ctx, in, target, source, jobName, true)
}

// writeIncrOnce 在单个事务中完成增量写入（append / merge 共用）。
// needDelete=true 时先按 PK 从目标表删除重复行（merge 语义），false 时仅追加（append 语义）。
func (d *pgWriterDialect) writeIncrOnce(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string, needDelete bool) error {
	firstBatch, foundRows := drainFirstBatch(in)

	if !foundRows {
		log.Printf("table=%s no incremental rows, skip", target.Table)
		return nil
	}

	tx, err := d.conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	staging := buildTempTableName(target.Table)

	if err := d.createTempTable(ctx, tx, staging, target); err != nil {
		return err
	}

	if err := d.writeCopyWithFirstBatch(ctx, firstBatch, in, staging, tx.Conn()); err != nil {
		return err
	}

	var deleted int64
	if needDelete {
		deleted, err = d.deleteTarget(ctx, tx, staging, target)
		if err != nil {
			return err
		}
	}

	inserted, err := d.insertTarget(ctx, tx, staging, target)
	if err != nil {
		return err
	}

	watermark := ""
	if source != nil && source.IncrField != "" {
		maxWM, err := d.computeWatermark(ctx, tx, staging, source)
		if err != nil {
			return err
		}

		if err := d.updateWatermark(ctx, tx, maxWM, target, source, jobName); err != nil {
			return err
		}

		watermark = " watermark=" + maxWM
	}

	if needDelete {
		log.Printf("table=%s deleted=%d inserted=%d%s", target.Table, deleted, inserted, watermark)
	} else {
		log.Printf("table=%s appended=%d%s", target.Table, inserted, watermark)
	}

	return tx.Commit(ctx)
}

// writeIncrChunked 将增量写入拆成若干块，每 CommitBatchSize 个 batch 提交一次事务并更新水位。
// needDelete=true 时每块先 DELETE 再 INSERT（merge），false 时仅 INSERT（append）。
// 适用于超大表：中断后重启可从上次已提交的水位断点继续，而不必从头同步。
func (d *pgWriterDialect) writeIncrChunked(ctx context.Context, in <-chan reader.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string, needDelete bool) error {
	var totalDeleted, totalInserted int64
	chunkIdx := 0

	// 当前块的状态
	var (
		currentTx      pgx.Tx
		currentStaging string
		feedCh         chan reader.Batch
		copyDone       chan error
		batchCount     int
	)

	startChunk := func(columns []string) error {
		chunkIdx++
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
		feedCh = make(chan reader.Batch, 1)
		copyDone = make(chan error, 1)
		go func(txConn *pgx.Conn) {
			copyDone <- d.writeCopyStream(ctx, staging, columns, txConn, func(write func(reader.Batch) error) error {
				for b := range feedCh {
					if err := write(b); err != nil {
						return err
					}
				}
				return nil
			})
		}(tx.Conn())
		return nil
	}

	commitChunk := func() error {
		close(feedCh)

		// 必须等待 COPY goroutine 完全结束，才能在同一连接上执行后续 SQL
		if copyErr := <-copyDone; copyErr != nil {
			_ = currentTx.Rollback(ctx)
			return fmt.Errorf("table=%s COPY failed: %w", target.Table, util.WrapPgError(copyErr))
		}

		var deleted int64
		if needDelete {
			var err error
			deleted, err = d.deleteTarget(ctx, currentTx, currentStaging, target)
			if err != nil {
				_ = currentTx.Rollback(ctx)
				return err
			}
		}

		inserted, err := d.insertTarget(ctx, currentTx, currentStaging, target)
		if err != nil {
			_ = currentTx.Rollback(ctx)
			return err
		}
		totalDeleted += deleted
		totalInserted += inserted

		watermark := ""
		committedWM := ""
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
			committedWM = maxWM
			watermark = " watermark=" + maxWM
		}

		if needDelete {
			log.Printf("table=%s chunk#%d: deleted=%d inserted=%d%s", target.Table, chunkIdx, deleted, inserted, watermark)
		} else {
			log.Printf("table=%s chunk#%d: appended=%d%s", target.Table, chunkIdx, inserted, watermark)
		}

		if err := currentTx.Commit(ctx); err != nil {
			return err
		}

		// fix: 本块提交成功后，把内存中的水位推进到本块的最大值。
		// 若后续块失败并触发外层 util.Retry 重跑整条 pipeline，runPipeline 会复用这个已推进的 IncrPoint，
		// Reader 从已提交的断点续传，而不是从任务起始水位重新抽取，从而避免已成功的分片被重复导入。
		if committedWM != "" {
			source.IncrPoint = committedWM
		}
		// 模拟异常，测试 retry 机制
		// return fmt.Errorf("simulated error for retry mechanism")
		return nil
	}

	for batch := range in {
		if len(batch.Rows) == 0 {
			continue
		}
		if batchCount == 0 {
			if err := startChunk(batch.ColumnNames()); err != nil {
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
				return fmt.Errorf("table=%s copy goroutine failed: %w", target.Table, util.WrapPgError(copyErr))
			}
			return fmt.Errorf("table=%s copy goroutine exited unexpectedly", target.Table)
		}

		if batchCount >= target.CommitBatchSize {
			if err := commitChunk(); err != nil {
				return err
			}
			batchCount = 0
		}
	}

	if batchCount > 0 {
		if err := commitChunk(); err != nil {
			return err
		}
	}

	if chunkIdx == 0 {
		log.Printf("table=%s no incremental rows, skip", target.Table)
	} else if needDelete {
		log.Printf("table=%s total: deleted=%d inserted=%d chunks=%d",
			target.Table, totalDeleted, totalInserted, chunkIdx)
	} else {
		log.Printf("table=%s total: appended=%d chunks=%d",
			target.Table, totalInserted, chunkIdx)
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

	// CREATE TEMP TABLE ... (LIKE target) 会引用目标表：目标表不存在等结构性错误（42xxx）
	// 重试无益，交给 WrapPgError 归一化为 NonRetryable，避免无谓重试。
	return util.WrapPgError(err)
}

// tryTruncateWithTimeout 尝试在事务内执行带 lock_timeout 的 TRUNCATE。
// 若 TRUNCATE 因锁等待超时失败，返回 lock_timeout 错误（55P03），由调用方决定后续退避策略。
// timeout > 0 时设置 lock_timeout，timeout <= 0 时不设超时直接 TRUNCATE。
func (d *pgWriterDialect) tryTruncateWithTimeout(ctx context.Context, tx pgx.Tx, target *config.TargetConfig) error {
	timeout := target.TruncateTimeout

	if timeout > 0 {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL lock_timeout = '%ds'", timeout)); err != nil {
			return err
		}
	}

	_, err := tx.Exec(ctx, fmt.Sprintf("TRUNCATE TABLE %s", target.Table))
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
		// 结构性错误（如目标表不存在 42P01）重试无益，归一化为 NonRetryable。
		return 0, util.WrapPgError(err)
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
		// 结构性错误（如目标表不存在 42P01）重试无益，归一化为 NonRetryable。
		return 0, util.WrapPgError(err)
	}

	return tag.RowsAffected(), nil
}

func (d *pgWriterDialect) computeWatermark(ctx context.Context, tx pgx.Tx, staging string, source *config.SourceConfig) (string, error) {
	// 暂存表结构 LIKE 目标表，列名为映射后的目标名；
	// 若增量字段经 fields_mapping 改名，须用目标列名聚合，否则报列不存在。
	incrColumn := source.FieldsMapping.TargetColumn(source.IncrField)
	sql := fmt.Sprintf(
		`SELECT COALESCE(MAX(%s)::text, '') FROM %s`,
		incrColumn,
		staging,
	)

	var wm string
	err := tx.QueryRow(ctx, sql).Scan(&wm)
	return wm, util.WrapPgError(err)
}

func (d *pgWriterDialect) updateWatermark(ctx context.Context, tx pgx.Tx, wm string, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	// 与暂存表写入在同一事务内提交，保证「数据落地」与「水位推进」原子一致。
	return upsertWatermark(ctx, tx, wm, target, source, jobName)
}

func (d *pgWriterDialect) getWatermark(target *config.TargetConfig, source *config.SourceConfig, jobName string) (string, error) {
	ctx := context.Background()
	wm, err := readWatermarkPoint(ctx, d.conn, target, source, jobName)
	if err != nil {
		return "", err
	}

	// 如果 job_data_sync 中没有记录或 incr_point 为空，回退到从目标表查增量字段最大值
	if wm == "" && source.IncrField != "" {
		ctx := context.Background()
		// 目标表列名为映射后的目标名，须用目标列名聚合，否则源字段改名后会报列不存在。
		incrColumn := source.FieldsMapping.TargetColumn(source.IncrField)
		fallbackSQL := fmt.Sprintf(`SELECT COALESCE(MAX(%s)::text, '') FROM %s`, incrColumn, target.Table)
		var fallback string
		if err := d.conn.QueryRow(ctx, fallbackSQL).Scan(&fallback); err != nil {
			return "", fmt.Errorf("fallback watermark query failed: %w", err)
		}
		if fallback != "" {
			log.Printf("watermark fallback: using MAX(%s)=%s from table %s", incrColumn, fallback, target.Table)
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
			fmt.Sprintf("%s.%s=%s.%s", t1, strings.TrimSpace(c), t2, strings.TrimSpace(c)))
	}

	return strings.Join(parts, " AND ")
}
