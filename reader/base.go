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
	buildBaseQuery(source *config.SourceConfig, projection string, whereClause string) (string, error)
	getColumnHandler(dbType string) ColHandler
	// formatIncrValue 将增量水位字符串格式化为可直接嵌入 SQL 的字面量或函数调用。
	formatIncrValue(v string) string
	quoteIdentifier(identifier string) string
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
	query, err := r.buildBaseQuery(true)
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

func (r *BaseReader) buildReadQuery() (string, error) {
	query, err := r.buildBaseQuery(false)
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

func (r *BaseReader) buildBaseQuery(emptyResult bool) (string, error) {
	projection, err := r.resolveProjection()
	if err != nil {
		return "", err
	}

	whereClause := r.buildWhereClause(emptyResult)
	return r.dialect.buildBaseQuery(r.Source, projection, whereClause)
}

func (r *BaseReader) buildWhereClause(emptyResult bool) string {
	if emptyResult {
		return "1=0"
	}

	parts := make([]string, 0, 2)
	if cond := strings.TrimSpace(r.Source.WhereStatement); cond != "" {
		parts = append(parts, fmt.Sprintf("(%s)", cond))
	}
	parts = append(parts, "1=1")
	return strings.Join(parts, " AND ")
}

// resolveProjection 生成 SQL SELECT 子句中的列投影部分。
// - 无字段映射时返回 "*"（查询所有列）
// - 有字段映射时生成 "col1 AS alias1, col2 AS alias2, ..." 格式
// 采用回调模式将字段格式化操作下沉到方言层（PostgreSQL/MSSQL），
// 确保不同数据库的引号规则（" vs []）得以正确应用。
func (r *BaseReader) resolveProjection() (string, error) {
	if r.Source == nil {
		return "*", nil
	}

	if r.Source.FieldsMapping.IsEmpty() {
		return "*", nil
	}

	return r.Source.FieldsMapping.Projection(
		func(sourceField string) string {
			return formatProjectionSource(sourceField, r.dialect.quoteIdentifier)
		},
		func(targetField string) string {
			return formatProjectionAlias(targetField, r.dialect.quoteIdentifier)
		},
	)
}

// formatProjectionSource 处理源表列名，区分两种情况：
//  1. 裸标识符（如 "id"、"user_name"）：交给 quoteIdentifier 加上方言特定的引号（PostgreSQL 用 ""，MSSQL 用 []）
//  2. 其他（已引用标识符或复杂表达式如 "CAST(x AS INT)"、`"ColumnName"`、"schema.table"）：直接返回
//     - 已引用标识符：quoteIdentifier 内部会幂等处理，与直接返回结果相同，无需单独分支
//     - 复杂表达式：不能加引号，否则会破坏表达式结构
func formatProjectionSource(sourceField string, quoteIdentifier func(string) string) string {
	sourceField = strings.TrimSpace(sourceField)
	if sourceField == "" {
		return ""
	}

	if isBareIdentifier(sourceField) {
		return quoteIdentifier(sourceField)
	}

	// 已引用标识符或复杂表达式，保持原样
	return sourceField
}

// formatProjectionAlias 处理目标列别名。
// 别名总是简单标识符（来自配置的 fields_mapping value），不支持复杂表达式，
// 直接交给 quoteIdentifier 处理——方言内部已做防重复引号检查，无需在此重复判断。
func formatProjectionAlias(alias string, quoteIdentifier func(string) string) string {
	alias = strings.TrimSpace(alias)
	if alias == "" {
		return "" // FieldsMapping.Projection 会检查此空值并报错
	}
	return quoteIdentifier(alias)
}

// isBareIdentifier 判断字符串是否为简单标识符（无需额外转义）。
// SQL 标准定义：首字符必须是字母或下划线，后续字符可以是字母、数字或下划线。
// 用途：区分 "id"（裸标识符）与 "CAST(x AS INT)" 或 "schema.table"（非裸标识符）。
// 裸标识符需要加引号以处理含有特殊字符或关键字的情况；
// 非裸标识符直接返回以保护其语法结构。
func isBareIdentifier(value string) bool {
	if value == "" {
		return false
	}
	for index, char := range value {
		if index == 0 {
			// 首字符：字母或下划线
			if (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') && char != '_' {
				return false
			}
			continue
		}
		// 后续字符：字母、数字或下划线
		if (char < 'A' || char > 'Z') && (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' {
			return false
		}
	}
	return true
}

// isAlreadyQuotedIdentifier 判断字符串是否已被引用，防止重复引号。
// PostgreSQL 风格引号：`"identifier"`
// MSSQL 风格引号：`[identifier]`
// 示例：`"UserId"` 或 `[UserId]` 已被引用，无需再加引号。
func isAlreadyQuotedIdentifier(value string) bool {
	return (strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)) ||
		(strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]"))
}

// defaultColumnHandler 是列值的通用转换器，负责将数据库查询结果转换为 CSV 格式的字符串。
//
// 核心设计：区分 nil（SQL NULL）与空字符串""
//   - nil 值 → 返回 util.NullSentinel（"__DB_ETL_NULL__"）
//     在 PostgreSQL COPY 中，此哨兵会被配置的 NULL '__DB_ETL_NULL__' 识别为真正的 SQL NULL
//   - 非 nil 值 → 通过 SanitizeString 进行 CSV 安全转义
//     空字符串会被 SanitizeString 返回为 ""（不是 NULL），保持在 COPY 中作为空字符串入库
//
// 这样做的原因：
// 之前的实现把 nil 都转为 ""（空字符串），导致 PostgreSQL 无法区分：
//
//	SELECT NULL → "" → COPY 中被当作普通空字符串，对 NOT NULL 约束无益
//
// 现在通过哨兵值中间层，确保：
//
//	SELECT NULL → NullSentinel → COPY 中被识别为 NULL，正确触发 NOT NULL 约束检查
//	SELECT '' → "" → COPY 中保持为空字符串，不触发 NOT NULL 约束
func defaultColumnHandler(v any) string {
	if v == nil {
		return util.NullSentinel
	}

	switch t := v.(type) {
	case []byte:
		return util.SanitizeString(string(t))
	case string:
		return util.SanitizeString(t)
	case time.Time:
		return t.Format("2006-01-02 15:04:05.000000")
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
