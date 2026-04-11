package writer

import (
	"context"
	"db-etl/config"
	"log"

	"github.com/jackc/pgx/v5"
)

func NewWriter(db config.DBConfig, dst *config.DownstreamConfig) Writer {
	switch db.Type {
	case config.DBTypePG, config.DBTypeGP:
		pgConn, err := pgx.Connect(context.Background(), db.DSN())
		if err != nil {
			log.Fatalf("PG connect failed: %v", err)
		}
		return &PGWriter{Conn: pgConn, Dst: dst}
	default:
		log.Fatalf("unsupported target db type: %s", db.Type)
	}
	return nil
}
