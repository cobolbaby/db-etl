package writer

import (
	"bytes"
	"context"
	"db-etl/config"
	"db-etl/transform"
	"io"
	"log"

	"github.com/jackc/pgx/v5"
)

type PGWriter struct {
	Conn  *pgx.Conn
	Table string
	Mode  config.ModeType
}

func (w *PGWriter) WriteBatch(in <-chan transform.CSVBatch) error {

	if w.Mode == config.ModeTypeCopy {
		pr, pw := io.Pipe()

		go func() {
			copySQL := "COPY " + w.Table + " FROM STDIN WITH (FORMAT CSV, DELIMITER ',', QUOTE '\"', ESCAPE '\"', NULL '')"
			if _, err := w.Conn.PgConn().CopyFrom(context.Background(), pr, copySQL); err != nil {
				log.Fatal(err)
			}
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
	}

	return nil
}
