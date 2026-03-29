package reader

import (
	"database/sql"
	"db-etl/config"
	"log"

	_ "github.com/microsoft/go-mssqldb"
)

func NewReader(dbType config.DBType, connStr string, rawsql string, table string, batchSize int) Reader {
	switch dbType {
	case config.DBTypeMSSQL:
		db, err := sql.Open("sqlserver", connStr)
		if err != nil {
			log.Fatalf("MSSQL connect failed: %v", err)
		}
		return &MSSQLReader{DB: db, SQL: rawsql, Table: table, BatchSize: batchSize}

	case config.DBTypePG:
		db, err := sql.Open("postgres", connStr)
		if err != nil {
			log.Fatalf("PG connect failed: %v", err)
		}
		return &PGReader{DB: db, SQL: rawsql, Table: table, BatchSize: batchSize}

	default:
		log.Fatalf("unsupported source db type: %s", dbType)
	}
	return nil
}
