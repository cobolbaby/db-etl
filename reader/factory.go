package reader

import (
	"database/sql"
	"db-etl/config"
	"log"

	_ "github.com/jackc/pgx/v5/stdlib"
	_ "github.com/microsoft/go-mssqldb"
)

func NewReader(db config.DBConfig, src *config.SourceConfig) Reader {
	switch db.Type {
	case config.DBTypeMSSQL:
		db, err := sql.Open("sqlserver", db.DSN())
		if err != nil {
			log.Fatalf("MSSQL connect failed: %v", err)
		}
		return NewMSSQLReader(db, src)

	case config.DBTypePG, config.DBTypeGP:
		db, err := sql.Open("pgx", db.DSN())
		if err != nil {
			log.Fatalf("PG connect failed: %v", err)
		}
		return NewPGReader(db, src)
	default:
		log.Fatalf("unsupported source db type: %s", db.Type)
	}
	return nil
}
