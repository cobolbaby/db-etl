package reader

import (
	"context"
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"strings"
	"time"
)

type readerDialect interface {
	buildBaseQuery(source *config.SourceConfig, projection string, whereClause string) (string, error)
	getColumnHandler(dbType string) ColHandler
	quoteIdentifier(identifier string) string
	// wrapError 按方言对底层驱动错误做归一化，将无法通过重试解决的错误
	//（语法错误、无效列名、约束冲突等）标记为 NonRetryable。
	wrapError(err error) error
}

type BaseReader struct {
	DB           *sql.DB
	Source       *config.SourceConfig
	QueryTimeout time.Duration // 0 表示不限制
	dialect      readerDialect
	err          error // ReadBatch 异步执行期间捕获的错误，通过 Err() 暴露
}

// Err 返回 ReadBatch 异步读取期间发生的错误，应在 channel 耗尽后调用。
func (r *BaseReader) Err() error { return r.err }

// queryContext 基于 parent 返回一个带超时的 context，若 QueryTimeout == 0 则直接返回 parent。
func (r *BaseReader) queryContext(parent context.Context) (context.Context, context.CancelFunc) {
	if r.QueryTimeout > 0 {
		return context.WithTimeout(parent, r.QueryTimeout)
	}
	return parent, func() {}
}

func (r *BaseReader) GetColumnHandlers() ([]ColHandler, error) {
	colTypes, err := r.getColumnTypes()
	if err != nil {
		return nil, err
	}

	handlers := make([]ColHandler, len(colTypes))
	for i, ct := range colTypes {
		handlers[i] = r.dialect.getColumnHandler(ct.DatabaseTypeName())
	}
	return handlers, nil
}

func (r *BaseReader) getColumnTypes() ([]*sql.ColumnType, error) {
	query, err := r.buildBaseQuery(true)
	if err != nil {
		return nil, err
	}

	ctx, cancel := r.queryContext(context.Background())
	defer cancel()

	rows, err := r.DB.QueryContext(ctx, query)
	if err != nil {
		return nil, r.dialect.wrapError(fmt.Errorf("%w，sql: %s", err, query))
	}
	defer rows.Close()

	return rows.ColumnTypes()
}

func (r *BaseReader) ReadBatch(ctx context.Context, cancel context.CancelFunc) <-chan RowBatch {
	out := make(chan RowBatch, 8)
	go func() {
		defer close(out)

		// fail 记录错误并 cancel：后者会令 writer 正在进行的事务以 context.Canceled
		// 中止并回滚，避免在 full/copy 等模式下提交被截断的部分数据。
		fail := func(err error) {
			r.err = err
			cancel()
		}

		query, err := r.buildReadQuery()
		if err != nil {
			// 构建查询失败属于配置错误，重试无益，标记为不可重试。
			fail(util.NonRetryable(fmt.Errorf("build query: %w，sql: %s", err, query)))
			return
		}

		// log.Println("query: ", query)

		qctx, qcancel := r.queryContext(ctx)
		defer qcancel()

		rows, err := r.DB.QueryContext(qctx, query)
		if err != nil {
			fail(r.dialect.wrapError(fmt.Errorf("execute query: %w，sql: %s", err, query)))
			return
		}
		defer rows.Close()

		batchSize := r.Source.BatchSize

		cols, err := rows.Columns()
		if err != nil {
			fail(r.dialect.wrapError(fmt.Errorf("get columns: %w，sql: %s", err, query)))
			return
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
					fail(r.dialect.wrapError(fmt.Errorf("scan row: %w，sql: %s", err, query)))
					return
				}
				batch = append(batch, values)
			}

			if err := rows.Err(); err != nil {
				fail(r.dialect.wrapError(fmt.Errorf("iterate rows: %w，sql: %s", err, query)))
				return
			}

			if len(batch) == 0 {
				break
			}

			// 监听 ctx：若下游（writer/transform）已失败并 cancel，及时退出避免 goroutine 泄漏。
			select {
			case out <- RowBatch{Columns: cols, Rows: batch}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func (r *BaseReader) buildReadQuery() (string, error) {
	query, err := r.buildBaseQuery(false)
	if err != nil {
		return "", err
	}

	// ORDER BY 在占位符替换之前拼接，使 order_by 中的 ${SRC_INCR_FIELD}
	// 与 WHERE 中的占位符共用同一套方言加引号逻辑（full 模式不排序）。
	if r.Source.Mode != config.ModeTypeFull && r.Source.OrderBy != "" {
		query += " ORDER BY " + r.Source.OrderBy
	}

	// 占位符一旦出现就必须替换，否则残留的 ${...} 会被数据库当成非法语法（syntax error at or near "$"）。
	// 这与同步模式无关：即便是 full/copy 模式，只要 where 里写了占位符也要替换掉。
	hasPlaceholder := strings.Contains(query, "${SRC_INCR_FIELD}") ||
		strings.Contains(query, "${INCR_POINT}")
	if hasPlaceholder {
		if strings.Contains(query, "${SRC_INCR_FIELD}") && r.Source.IncrField == "" {
			return "", fmt.Errorf("query uses ${SRC_INCR_FIELD} but incr_field is empty")
		}
		// IncrPoint 为空时替换会产出形如 `> ''` 的非法 SQL（数据库随后报 22P02）。
		// 空值通常意味着未设 incr_field（watermark/默认值链路被跳过），属配置错误，直接失败。
		if strings.Contains(query, "${INCR_POINT}") && r.Source.IncrPoint == "" {
			return "", fmt.Errorf("query uses ${INCR_POINT} but incr_point is empty; set incr_field for watermark tracking or provide an initial incr_point")
		}
		// SRC_INCR_FIELD 是列引用，与投影一样按方言加引号，
		// 否则含空格/中文等的字段名（如 "Test Finish Date"）拼进 WHERE 会触发语法错误。
		incrField := formatProjectionSource(r.Source.IncrField, r.dialect.quoteIdentifier)
		query = strings.ReplaceAll(query, "${SRC_INCR_FIELD}", incrField)
		query = strings.ReplaceAll(query, "${INCR_POINT}", r.Source.IncrPoint)
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

	if cond := strings.TrimSpace(r.Source.WhereStatement); cond != "" {
		return cond
	}
	return "1=1"
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

	// 已被引用的标识符（[x] 或 "x"）直接返回：外层绝不能再套一层引号，
	// 否则会产出 [[x]] / ""x"" 这类非法标识符。
	if isAlreadyQuotedIdentifier(sourceField) {
		return sourceField
	}

	if isColumnIdentifier(sourceField) {
		return quoteIdentifier(sourceField)
	}

	// SQL 表达式（如 GETDATE()、CAST(...)）或限定名（如 dbo.tbl），保持原样
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

// isColumnIdentifier 判断 source 字段是否应作为“列标识符”整体加引号。
// 调用前 formatProjectionSource 已把「已被引用的标识符」提前返回，故此处只需区分
// 「纯列名」与「SQL 表达式/限定名」。采用保守启发式：只要不含 SQL 表达式特征字符
// （圆括号、点号），就视为纯列名并加引号——这样即可覆盖含空格、非 ASCII 字符（如中文）、
// 斜杠等的列名，例如 "Analysis Result Judge"、"故障DC/LC"、"Cost Saving"。
// 反之：
//   - 含 "(" 或 ")"：视为函数调用/表达式（如 GETDATE()、CAST(x AS INT)），保持原样不加引号；
//   - 含 "."：视为限定名（如 dbo.tbl、schema.column），保持原样。
//
// 注意：含圆括号的列名（如 "Price(USD)"、"等級(Level)"）会被当作表达式而不加引号，
// 这类列名需在配置中预先自行引用（如写成 "[Price(USD)]"）或改用 src_rawsql。
func isColumnIdentifier(value string) bool {
	if value == "" {
		return false
	}
	return !strings.ContainsAny(value, "().")
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
