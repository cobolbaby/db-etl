package reader

import (
	"database/sql"
	"log"

	_ "github.com/microsoft/go-mssqldb"
)

func NewReader(dbType string, connStr string, table string, batchSize int) Reader {
	switch dbType {
	case "mssql":
		db, err := sql.Open("sqlserver", connStr)
		if err != nil {
			log.Fatalf("MSSQL connect failed: %v", err)
		}
		return &MSSQLReader{DB: db, Table: table, BatchSize: batchSize}

	default:
		log.Fatalf("unsupported source db type: %s", dbType)
	}
	return nil
}
