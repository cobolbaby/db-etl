package writer

import (
	"context"
	"db-etl/config"
	"log"

	"github.com/jackc/pgx/v5"
)

func NewWriter(db config.DBConfig, target *config.TargetConfig, jobName string) Writer {
	switch db.Type {
	case config.DBTypePG, config.DBTypeGP:
		pgConn, err := pgx.Connect(context.Background(), db.DSN())
		if err != nil {
			log.Fatalf("PG connect failed: %v", err)
		}
		return NewPGWriter(pgConn, target, jobName)
	default:
		log.Fatalf("unsupported target db type: %s", db.Type)
	}
	return nil
}
