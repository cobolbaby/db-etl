package reader

import (
	"context"
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

type readerDialect interface {
	buildBaseQuery(source *config.SourceConfig, emptyResult bool) (string, error)
	getColumnHandler(dbType string) ColHandler
	// formatIncrValue 将增量水位字符串格式化为可直接嵌入 SQL 的字面量或函数调用。
	formatIncrValue(v string) string
}

type BaseReader struct {
	DB           *sql.DB
	Source       *config.SourceConfig
	QueryTimeout time.Duration // 0 表示不限制
	dialect      readerDialect
}

// queryContext 返回一个带超时的 context，若 QueryTimeout == 0 则返回 background context。
func (r *BaseReader) queryContext() (context.Context, context.CancelFunc) {
	if r.QueryTimeout > 0 {
		return context.WithTimeout(context.Background(), r.QueryTimeout)
	}
	return context.Background(), func() {}
}

func (r *BaseReader) ReadBatch() <-chan RowBatch {
	out := make(chan RowBatch, 8)
	go func() {
		defer close(out)

		query, err := r.buildReadQuery()
		if err != nil {
			log.Fatalf("build query: %v，sql: %s", err, query)
		}

		// log.Println("query: ", query)

		ctx, cancel := r.queryContext()
		defer cancel()

		rows, err := r.DB.QueryContext(ctx, query)
		if err != nil {
			log.Fatalf("execute query: %v，sql: %s", err, query)
		}
		defer rows.Close()

		batchSize := r.Source.BatchSize

		cols, err := rows.Columns()
		if err != nil {
			log.Fatalf("get columns: %v，sql: %s", err, query)
		}

		for {
			batch := make([][]any, 0, batchSize)
			for len(batch) < batchSize && rows.Next() {
				values := make([]any, len(cols))
				valuePtrs := make([]any, len(cols))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					log.Fatalf("scan row: %v，sql: %s", err, query)
				}
				batch = append(batch, values)
			}

			if err := rows.Err(); err != nil {
				log.Fatalf("iterate rows: %v，sql: %s", err, query)
			}

			if len(batch) == 0 {
				break
			}
			out <- RowBatch{Columns: cols, Rows: batch}
		}
	}()
	return out
}

func (r *BaseReader) GetColumnHandlers() []ColHandler {
	colTypes, err := r.getColumnTypes()
	if err != nil {
		log.Fatalf("get column types: %v", err)
	}

	handlers := make([]ColHandler, len(colTypes))
	for i, ct := range colTypes {
		handlers[i] = r.dialect.getColumnHandler(ct.DatabaseTypeName())
	}
	return handlers
}

func (r *BaseReader) getColumnTypes() ([]*sql.ColumnType, error) {
	query, err := r.dialect.buildBaseQuery(r.Source, true)
	if err != nil {
		return nil, err
	}

	ctx, cancel := r.queryContext()
	defer cancel()

	rows, err := r.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return rows.ColumnTypes()
}

func (r *BaseReader) buildReadQuery() (string, error) {
	query, err := r.dialect.buildBaseQuery(r.Source, false)
	if err != nil {
		return "", err
	}

	if r.Source.Mode == config.ModeTypeFull || r.Source.IncrField == "" {
		return query, nil
	}

	if strings.Contains(query, "${SRC_INCR_FIELD}") ||
		strings.Contains(query, "${INCR_POINT}") {
		query = strings.ReplaceAll(query, "${SRC_INCR_FIELD}", r.Source.IncrField)
		query = strings.ReplaceAll(query, "${INCR_POINT}", r.Source.IncrPoint)
		// 占位符模式下用户 SQL 自行控制排序，不再追加
		return query, nil
	}

	cond := fmt.Sprintf("%s > %s", r.Source.IncrField, r.dialect.formatIncrValue(r.Source.IncrPoint))
	query = query + " AND " + cond

	if r.Source.OrderBy != "" {
		query += " ORDER BY " + r.Source.OrderBy
	}

	return query, nil
}

func defaultColumnHandler(v any) string {
	if v == nil {
		return ""
	}

	switch t := v.(type) {
	case []byte:
		return util.SanitizeString(string(t))
	case string:
		return util.SanitizeString(t)
	default:
		return util.SanitizeString(fmt.Sprintf("%v", t))
	}
}

func isNumericLiteral(s string) bool {
	if _, err := strconv.ParseInt(s, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	return false
}

// isDateTimeLiteral 判断字符串是否为日期/时间字面量。
// 支持 "2006-01-02 15:04:05"、"2006-01-02 15:04:05.999999999" 等常见格式。
func isDateTimeLiteral(s string) bool {
	formats := []string{
		"2006-01-02 15:04:05",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02T15:04:05.999999999Z07:00",
	}
	for _, f := range formats {
		if _, err := time.Parse(f, s); err == nil {
			return true
		}
	}
	return false
}
