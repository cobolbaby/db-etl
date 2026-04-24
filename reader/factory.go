package reader

import (
	"database/sql"
	"db-etl/config"
	"log"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
)

func NewReader(db config.DBConfig, src *config.SourceConfig) Reader {
	queryTimeout := time.Duration(db.QueryTimeout) * time.Second

	switch db.Type {
	case config.DBTypeMSSQL:
		conn, err := sql.Open("sqlserver", db.DSN())
		if err != nil {
			log.Fatalf("MSSQL connect failed: %v", err)
		}
		return NewMSSQLReader(conn, src, queryTimeout)

	case config.DBTypePG, config.DBTypeGP:
		conn, err := sql.Open("pgx", db.DSN())
		if err != nil {
			log.Fatalf("PG connect failed: %v", err)
		}
		return NewPGReader(conn, src, queryTimeout)
	default:
		log.Fatalf("unsupported source db type: %s", db.Type)
	}
	return nil
}
