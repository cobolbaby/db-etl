package reader

import (
	"context"
	"database/sql"
	"db-etl/config"
	"db-etl/util"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type readerDialect interface {
	buildBaseQuery(source *config.SourceConfig, projection string, whereClause string) (string, error)
	// columnKind 将本方言的类型名映射到归一化的 ColumnKind。
	columnKind(dbType string) ColumnKind
	// valueNormalizer 返回该类型所需的值修正函数；无需修正时返回 nil。
	// loc 为源库时区，供无时区时间列的墙钟解释使用。
	valueNormalizer(dbType string, loc *time.Location) ValueNormalizer
	quoteIdentifier(identifier string) string
	// wrapError 按方言对底层驱动错误做归一化，将无法通过重试解决的错误
	//（语法错误、无效列名、约束冲突等）标记为 NonRetryable。
	wrapError(err error) error
}

type BaseReader struct {
	conn    *sql.DB
	Source  *config.SourceConfig
	dialect readerDialect
	// loc 为源库时区（由 config.DBConfig.Location 解析），
	// 用于把无时区时间列的墙钟解释为该时区；nil 时回退 time.Local。
	loc *time.Location
	err error // ReadBatch 异步执行期间捕获的错误，通过 Err() 暴露
}

// Err 返回 ReadBatch 异步读取期间发生的错误，应在 channel 耗尽后调用。
func (r *BaseReader) Err() error { return r.err }

// Close 释放底层数据库连接。
func (r *BaseReader) Close() error {
	if r.conn != nil {
		return r.conn.Close()
	}
	return nil
}

// columnMeta 将结果集的列描述转为列名、源库类型名与归一化语义类别，顺序与查询列一致。
// 三者同源于抽取查询自身的结果集描述，无需额外往返源库探测。
func (r *BaseReader) columnMeta(colTypes []*sql.ColumnType) []ColumnMeta {
	meta := make([]ColumnMeta, len(colTypes))
	for i, ct := range colTypes {
		typeName := ct.DatabaseTypeName()
		meta[i] = ColumnMeta{
			Name:     ct.Name(),
			TypeName: typeName,
			Kind:     r.dialect.columnKind(typeName),
		}
	}
	return meta
}

func (r *BaseReader) ReadBatch(ctx context.Context, cancel context.CancelFunc) <-chan Batch {
	out := make(chan Batch, 8)
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

		rows, err := r.conn.QueryContext(ctx, query)
		if err != nil {
			fail(r.dialect.wrapError(fmt.Errorf("execute query: %w，sql: %s", err, query)))
			return
		}
		defer rows.Close()

		batchSize := r.Source.BatchSize

		colTypes, err := rows.ColumnTypes()
		if err != nil {
			fail(r.dialect.wrapError(fmt.Errorf("get columns: %w，sql: %s", err, query)))
			return
		}

		// 列元数据取自本次查询的结果集描述，随每个批次下发；
		columns := r.columnMeta(colTypes)

		// 驱动层的值表示差异在此归一，使下游只需面对 ColumnKind 约定的 Go 类型。
		// 绝大多数列无需修正（normalizer 为 nil），故先探测是否有任何一列需要，
		// 避免为空操作在每行上多走一遍循环。
		normalizers := make([]ValueNormalizer, len(colTypes))
		needNormalize := false
		for i, ct := range colTypes {
			if n := r.dialect.valueNormalizer(ct.DatabaseTypeName(), r.loc); n != nil {
				normalizers[i] = n
				needNormalize = true
			}
		}

		for {
			batch := make([][]any, 0, batchSize)
			for len(batch) < batchSize && rows.Next() {
				values := make([]any, len(colTypes))
				valuePtrs := make([]any, len(colTypes))
				for i := range values {
					valuePtrs[i] = &values[i]
				}
				if err := rows.Scan(valuePtrs...); err != nil {
					fail(r.dialect.wrapError(fmt.Errorf("scan row: %w，sql: %s", err, query)))
					return
				}
				if needNormalize {
					for i, n := range normalizers {
						if n != nil && values[i] != nil {
							values[i] = n(values[i])
						}
					}
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
			case out <- Batch{Columns: columns, Rows: batch}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func (r *BaseReader) buildReadQuery() (string, error) {
	projection, err := r.resolveProjection()
	if err != nil {
		return "", err
	}

	whereClause := r.buildWhereClause()
	query, err := r.dialect.buildBaseQuery(r.Source, projection, whereClause)
	if err != nil {
		return "", err
	}

	// 仅增量模式（append/merge）才需要有序读取：
	// 增量小批次依赖 ORDER BY 才能按水位/断点续传稳定推进，table 与 sql（rawsql）源都需要；
	// 全量模式（full/initial）无需排序，避免大表顺扫叠加排序开销。
	// ORDER BY 在占位符替换之前拼接，使 order_by 中的 ${SRC_INCR_FIELD} 与 WHERE 中的占位符共用同一套方言加引号逻辑。
	if (r.Source.Mode == config.ModeTypeAppend || r.Source.Mode == config.ModeTypeMerge) &&
		r.Source.OrderBy != "" {
		query += " ORDER BY " + r.Source.OrderBy
	}

	// 占位符一旦出现就必须替换，否则残留的 ${...} 会被数据库当成非法语法（syntax error at or near "$"）。
	// 这与同步模式无关：即便是 full/initial 模式，只要 SQL/where 里写了占位符也要替换掉；
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
		// 兼容 where_statement 作者已把占位符包在引号里的历史写法（"${SRC_INCR_FIELD}" / [${SRC_INCR_FIELD}]）：
		// 框架已对字段自动加引号，若再叠加引号会产出 ""x"" / [[x]] 这类非法标识符。故先整体替换带引号的占位符，再处理裸占位符。
		query = strings.ReplaceAll(query, `"${SRC_INCR_FIELD}"`, incrField)
		query = strings.ReplaceAll(query, "[${SRC_INCR_FIELD}]", incrField)
		query = strings.ReplaceAll(query, "${SRC_INCR_FIELD}", incrField)
		query = strings.ReplaceAll(query, "${INCR_POINT}", r.Source.IncrPoint)
	}

	return query, nil
}

func (r *BaseReader) buildWhereClause() string {
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

// formatProjectionSource 处理源表列名，按序依次判断，命中即原样返回，最后兜底加引号：
//  1. 已被引号/方括号包裹的标识符（如 `"ColumnName"`、"[Price(USD)]"：直接返回，
//     quoteIdentifier 内部也会幂等处理，与直接返回结果相同，无需单独分支。
//  2. 单引号包裹的 SQL 字符串字面量（如 "'tao'"）：用于配置「常量列」（fields_mapping 里
//     "'tao'": src_region 生成 'tao' AS src_region），原样透传，不当列名处理。
//  3. 数字常量（如 "1"、"-1"、"1.5"）：原样透传，不支持纯数字列名。
//  4. SQL 表达式（如 "GETDATE()"、"CAST(x AS INT)"）：不能加引号，否则会破坏表达式结构，
//     原样透传。
//  5. 以上均不命中：视为裸标识符（如 "id"、"user_name"），交给 quoteIdentifier 加上
//     方言特定的引号（PostgreSQL 用 ""，MSSQL 用 []）。
func formatProjectionSource(sourceField string, quoteIdentifier func(string) string) string {
	sourceField = strings.TrimSpace(sourceField)
	if sourceField == "" {
		return ""
	}

	// 已被包裹的标识符（[x] 或 "x"）直接返回：外层绝不能再套一层引号，
	// 否则会产出 [[x]] / ""x"" 这类非法标识符。
	if isAlreadyQuotedIdentifier(sourceField) {
		return sourceField
	}

	// 单引号字符串字面量是 SQL 标准的「常量值」语法（跨 PostgreSQL/MSSQL/Oracle 通用），
	// 必须先于下面的纯列名判断，否则会被当作裸列名误加方言引号
	// （如 PostgreSQL 下变成 "'tao'"，变成一个不存在的列名而非常量）。
	if isStringLiteral(sourceField) {
		return sourceField
	}

	// 数字常量（如 "1"、"-1"、"1.5"）不支持作为列名加引号，否则形如 `${SRC_INCR_FIELD} > '${INCR_POINT}'`
	// 当 incr_field=1 时，1 就会被视为列名，触发 `column "1" does not exist`。
	// 若顺序颠倒会让小数走到裸标识符分支被错误加引号。
	if isNumericLiteral(sourceField) {
		return sourceField
	}

	// SQL 表达式（如 GETDATE()、CAST(...)）含圆括号，不能加引号，否则会破坏表达式结构，保持原样。
	if looksLikeExpression(sourceField) {
		return sourceField
	}

	// 剩下的纯列名（如 "id"、"user_name"，以及含空格/中文/斜杠等的列名），
	// 交给 quoteIdentifier 加上方言特定的引号（PostgreSQL 用 ""，MSSQL 用 []）。
	return quoteIdentifier(sourceField)
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

// isStringLiteral 判断字段是否为单引号包裹的 SQL 字符串字面量（如 "'tao'"），
// 用于在 fields_mapping 中配置常量列：{"'tao'": "src_region"} 生成 'tao' AS src_region。
// 注意：不处理字面量内部的转义单引号（”），这类取值建议改用 sql / src_rawsql 手写查询。
func isStringLiteral(value string) bool {
	return len(value) >= 2 && strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")
}

// looksLikeExpression 判断字段是否应被当作 SQL 表达式保持原样、不加引号。
// 采用保守启发式：只要含圆括号就判定为真（如 GETDATE()、CAST(x AS INT)这类函数调用/表达式）。
//
// 注意：
//   - 本心不处理未引用的限定名（如裸 dbo.col）——现实中 fields_mapping 的 key 是投影列，
//     本质上不存在需要写裸限定名的场景（该写法加上引号会把整个带点号的字符串当成
//     单一非法标识符）。若确实需要限定引用，请改写成已引用形式（如 "[dbo].[sourceId]"、
//     `"schema"."column"`），这类写法会被 isAlreadyQuotedIdentifier 识别并原样透传。
//   - 含圆括号的列名（如 "Price(USD)"、"等級(Level)"）也会被误判为表达式而不加引号，
//     这类列名需在配置中预先自行包裹（如写成 "[Price(USD)]"）或改用 src_rawsql。
func looksLikeExpression(value string) bool {
	return strings.ContainsAny(value, "()")
}

// isNumericLiteral 判断字段是否为数字常量（如 "1"、"-1"、"1.5"），这类值不支持作为列名加引号，
// 需保持原样：否则形如 `${SRC_INCR_FIELD} > '${INCR_POINT}'` 当 incr_field=1 时，
// 1 就会被当作列名加引号，触发 `column "1" does not exist`。
// 先用 ParseInt 快速命中整数；整数不命中时再用 ParseFloat 补充判断小数/科学记数法，
// 但排除 ParseFloat 能识别但并非真实数字列名的特殊词（NaN/Inf/Infinity），
// 避免字面上就是这些词的列名被误判为数字常量而漏掉引号包裹。
func isNumericLiteral(value string) bool {
	if _, err := strconv.ParseInt(value, 10, 64); err == nil {
		return true
	}
	if _, err := strconv.ParseFloat(value, 64); err == nil {
		switch strings.ToLower(value) {
		case "nan", "inf", "+inf", "-inf", "infinity", "+infinity", "-infinity":
			return false
		}
		return true
	}
	return false
}

// isAlreadyQuotedIdentifier 判断字符串是否已被包裹，防止重复引号。
// PostgreSQL 风格引号：`"identifier"`
// MSSQL 风格引号：`[identifier]`
// 示例：`"UserId"` 或 `[UserId]` 已被包裹，无需再加引号。
func isAlreadyQuotedIdentifier(value string) bool {
	return (strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)) ||
		(strings.HasPrefix(value, "[") && strings.HasSuffix(value, "]"))
}
