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
	pr, pw := io.Pipe()

	errCh := make(chan error, 1)
	copySQL := buildCopySQL(table, firstBatch.Columns)

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

		log.Println("COPY data sent, waiting for completion...")

		if err := <-errCh; err != nil {
			log.Println("COPY error:", err)
			return err
		}

		log.Println("COPY completed successfully")
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

	if err := writeBatch(firstBatch); err != nil {
		return finishWithError(err)
	}

	for batch := range in {
		if err := writeBatch(batch); err != nil {
			return finishWithError(err)
		}
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
	srcSchema, srcTable, err := watermarkSourceParts(source)
	if err != nil {
		return err
	}
	dstSchema, dstTable, err := watermarkTargetParts(target)
	if err != nil {
		return err
	}

	tag, err := tx.Exec(
		ctx,
		`UPDATE manager.job_data_sync
            SET incr_point = $1
          WHERE job_name = $2
		    AND src_schema_name = $3
		    AND src_table_name = $4
		    AND dst_schema_name = $5
		    AND dst_table_name = $6`,
		wm, funcName, srcSchema, srcTable, dstSchema, dstTable,
	)
	if err != nil {
		return err
	}

	if tag.RowsAffected() > 0 {
		return nil
	}

	_, err = tx.Exec(
		ctx,
		`INSERT INTO manager.job_data_sync
		    (job_name, src_schema_name, src_table_name, dst_schema_name, dst_table_name, incr_point)
		  VALUES ($1, $2, $3, $4, $5, $6)`,
		funcName, srcSchema, srcTable, dstSchema, dstTable, wm,
	)
	return err
}

func (d *pgWriterDialect) getWatermark(target *config.TargetConfig, source *config.SourceConfig, jobName string) (string, error) {
	funcName := watermarkJobName(jobName)
	srcSchema, srcTable, err := watermarkSourceParts(source)
	if err != nil {
		return "", err
	}
	dstSchema, dstTable, err := watermarkTargetParts(target)
	if err != nil {
		return "", err
	}

	var wm string
	err = d.conn.QueryRow(
		context.Background(),
		`SELECT COALESCE(incr_point, '')
           FROM manager.job_data_sync
          WHERE job_name = $1
		    AND src_schema_name = $2
		    AND src_table_name = $3
		    AND dst_schema_name = $4
		    AND dst_table_name = $5
          LIMIT 1`,
		funcName, srcSchema, srcTable, dstSchema, dstTable,
	).Scan(&wm)

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
	timeKeywords := []string{"cdt", "udt", "created", "updated", "modified", "ts"}
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
