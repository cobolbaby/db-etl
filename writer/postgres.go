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
	Conn  *pgx.Conn
	Table string
	Mode  config.ModeType
	DstPK string
}

func (w *PGWriter) WriteBatch(in <-chan transform.CSVBatch) error {

	switch w.Mode {

	case config.ModeTypeCopy:
		return w.copyToTable(in, w.Table)

	case config.ModeTypeMerge:

		if w.DstPK == "" {
			return fmt.Errorf("dst_pk is required for merge mode")
		}

		return w.copyAndMerge(in)

	default:
		return fmt.Errorf("unsupported mode: %s", w.Mode)
	}
}

func (w *PGWriter) copyToTable(in <-chan transform.CSVBatch, table string) error {
	pr, pw := io.Pipe()

	errCh := make(chan error, 1)

	go func() {
		copySQL := "COPY " + table + " FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '')"
		_, err := w.Conn.PgConn().CopyFrom(context.Background(), pr, copySQL)
		errCh <- err
	}()

	buf := bytes.NewBuffer(make([]byte, 0, 4*1024*1024))
	for batch := range in {
		for _, row := range batch.Rows {
			for i, col := range row {
				if i > 0 {
					buf.WriteByte(',')
				}
				buf.WriteString(col) // 简化版 CSV
			}
			buf.WriteByte('\n')
			if buf.Len() > 3*1024*1024 {
				pw.Write(buf.Bytes())
				buf.Reset()
			}
		}
	}
	if buf.Len() > 0 {
		pw.Write(buf.Bytes())
	}
	pw.Close()

	log.Println("COPY data sent, waiting for completion...")

	// 等待 COPY 完成
	if err := <-errCh; err != nil {
		log.Println("COPY error:", err)
		return err
	}

	log.Println("COPY completed successfully")
	return nil
}

func (w *PGWriter) copyAndMerge(in <-chan transform.CSVBatch) error {

	ctx := context.Background()
	tx, err := w.Conn.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	staging := buildTempTableName(w.Table)
	createSQL := fmt.Sprintf(
		"CREATE TEMP TABLE %s (LIKE %s INCLUDING DEFAULTS) ON COMMIT DROP;",
		staging, w.Table,
	)
	if _, err := tx.Exec(ctx, createSQL); err != nil {
		return err
	}

	// COPY 数据到 staging
	oldConn := w.Conn
	w.Conn = tx.Conn()

	if err := w.copyToTable(in, staging); err != nil {
		return err
	}

	w.Conn = oldConn

	// NOTICE: Greenplum 不支持 Merge/Upsert
	deleteSQL := fmt.Sprintf(
		`DELETE FROM %s t USING %s s WHERE %s`,
		w.Table, staging,
		buildJoinCondition("t", "s", w.DstPK),
	)
	if _, err := tx.Exec(ctx, deleteSQL); err != nil {
		return err
	}

	insertSQL := fmt.Sprintf(
		`INSERT INTO %s SELECT * FROM %s`,
		w.Table, staging,
	)
	if _, err := tx.Exec(ctx, insertSQL); err != nil {
		return err
	}

	return tx.Commit(ctx)
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
