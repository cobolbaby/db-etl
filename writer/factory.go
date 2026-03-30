package writer

import (
	"context"
	"db-etl/config"
	"log"

	"github.com/jackc/pgx/v5"
)

func NewWriter(dbType config.DBType, connStr string, table string, mode config.ModeType, dstPK string) Writer {
	switch dbType {
	case config.DBTypePG, config.DBTypeGP:
		pgConn, err := pgx.Connect(context.Background(), connStr)
		if err != nil {
			log.Fatalf("PG connect failed: %v", err)
		}
		return &PGWriter{Conn: pgConn, Table: table, Mode: mode, DstPK: dstPK}
	default:
		log.Fatalf("unsupported target db type: %s", dbType)
	}
	return nil
}
