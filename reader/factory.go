package reader

import (
	"database/sql"
	"db-etl/config"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
)

func NewReader(db config.DBConfig, src *config.SourceConfig, batchSize int) Reader {
	switch db.Type {
	case config.DBTypeMSSQL:
		db, err := sql.Open("sqlserver", db.DSN())
		if err != nil {
			log.Fatalf("MSSQL connect failed: %v", err)
		}
		return &MSSQLReader{DB: db, Src: src, BatchSize: batchSize}

	case config.DBTypePG, config.DBTypeGP:
		db, err := sql.Open("pgx", db.DSN())
		if err != nil {
			log.Fatalf("PG connect failed: %v", err)
		}
		return &PGReader{DB: db, Src: src, BatchSize: batchSize}
	default:
		log.Fatalf("unsupported source db type: %s", db.Type)
	}
	return nil
}
