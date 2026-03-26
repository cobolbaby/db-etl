package writer

import (
	"context"
	"log"

	"github.com/jackc/pgx/v5"
)

func NewWriter(dbType string, connStr string, table string) Writer {
	switch dbType {
	case "pg":
		pgConn, err := pgx.Connect(context.Background(), connStr)
		if err != nil {
			log.Fatalf("PG connect failed: %v", err)
		}
		return &PGWriter{Conn: pgConn, Table: table}
	default:
		log.Fatalf("unsupported target db type: %s", dbType)
	}
	return nil
}
