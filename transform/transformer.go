package transform

import (
	"db-etl/reader"
	"log"
	"sync"
)

// Transformer 是转换链上的一环：在 Batch 上做重塑（Batch -> Batch）。
// 因输入输出同构而可按顺序自由串接；pipeline 对每个批次只调用一次 Transform。
// 列元数据随 Batch 同行，故转换器无需在构造时预先获知源端列结构。
type Transformer interface {
	Transform(batch reader.Batch) reader.Batch
}

// passthrough 是无转换配置时的空实现：原样透传批次。
type passthrough struct{}

func (passthrough) Transform(batch reader.Batch) reader.Batch { return batch }

// chainTransformer 按顺序依次应用各个转换器。
type chainTransformer struct {
	steps []Transformer
}

func (t *chainTransformer) Transform(batch reader.Batch) reader.Batch {
	for _, step := range t.steps {
		batch = step.Transform(batch)
	}
	return batch
}

// UnpivotTransformer 将宽表列转行：把 Columns 中列出的源列展开成 KeyField/ValueField 两列的多行，
// 其余列作为标识列在每个展开行中重复保留。
// 把 unpivot 放在转换阶段（而非源端 SQL）可让源端只抽宽表，避免标识列在源库→ETL 链路上
// 随展开行数成倍重复传输。
type UnpivotTransformer struct {
	// KeyField 输出中承载“列标签”的列名。
	KeyField string
	// ValueField 输出中承载“列值”的列名。
	ValueField string
	// Columns 为「源列名 -> 写入 KeyField 的标签」映射；未命中的列作为标识列保留。
	Columns map[string]string
	// DropNull 为 true 时跳过源列值为 NULL 的展开行。
	DropNull bool

	// warnOnce 保证列布局异常（配置列缺失、输出列重名）只在首批告警一次，避免日志刷屏。
	warnOnce sync.Once
}

func (t *UnpivotTransformer) Transform(batch reader.Batch) reader.Batch {
	// 每批根据当前列名重新解析标识列与待展开列：转换器实例在多个 worker 间共享，
	// 将解析结果保留为局部变量（而非缓存到字段）可避免共享可变状态、天然并发安全；
	// 解析成本为 O(列数)，相对每批的行处理可忽略。
	type pivotCol struct {
		idx   int
		label string
	}
	idIdx := make([]int, 0, len(batch.Columns))
	idCols := make([]reader.ColumnMeta, 0, len(batch.Columns))
	pivots := make([]pivotCol, 0, len(t.Columns))
	resolved := make(map[string]struct{}, len(t.Columns))
	for i, col := range batch.Columns {
		if label, ok := t.Columns[col.Name]; ok {
			pivots = append(pivots, pivotCol{idx: i, label: label})
			resolved[col.Name] = struct{}{}
			continue
		}
		idIdx = append(idIdx, i)
		idCols = append(idCols, col)
	}

	t.warnOnce.Do(func() { t.checkLayout(idCols, resolved) })

	// KeyField 承载列标签，ValueField 汇聚多个异构源列的取值，二者都无单一源类型，
	// 固定为 KindString；取值则逐个按其源列的 Kind 渲染为规范文本后再汇入。
	outCols := make([]reader.ColumnMeta, 0, len(idCols)+2)
	outCols = append(outCols, idCols...)
	outCols = append(outCols,
		reader.ColumnMeta{Name: t.KeyField, Kind: reader.KindString},
		reader.ColumnMeta{Name: t.ValueField, Kind: reader.KindString},
	)

	outRows := make([][]any, 0, len(batch.Rows)*len(pivots))
	for _, row := range batch.Rows {
		// 标识列每输入行只取一次，供该行的多个展开行复用。
		idVals := make([]any, len(idIdx))
		for k, ci := range idIdx {
			idVals[k] = row[ci]
		}
		for _, p := range pivots {
			value := row[p.idx]
			if t.DropNull && value == nil {
				continue
			}
			rec := make([]any, 0, len(idVals)+2)
			rec = append(rec, idVals...)
			// nil 保持为 nil，令 NULL 语义完整传递到 writer。
			if value != nil {
				value = reader.FormatText(batch.Columns[p.idx].Kind, value)
			}
			rec = append(rec, p.label, value)
			outRows = append(outRows, rec)
		}
	}

	return reader.Batch{Columns: outCols, Rows: outRows}
}

// checkLayout 在首批数据上核对列布局，对易导致静默错误数据的配置问题发出告警：
//   - 配置的待展开列有部分/全部在实际结果中不存在（列名拼写或大小写不符）；
//   - key_field / value_field 与某个标识列同名，导致输出列重名、写入错列。
func (t *UnpivotTransformer) checkLayout(idCols []reader.ColumnMeta, resolved map[string]struct{}) {
	if len(resolved) != len(t.Columns) {
		missing := make([]string, 0, len(t.Columns)-len(resolved))
		for name := range t.Columns {
			if _, ok := resolved[name]; !ok {
				missing = append(missing, name)
			}
		}
		log.Printf("warning: unpivot resolved %d of %d configured columns; missing from source result: %v (check column name/case)",
			len(resolved), len(t.Columns), missing)
	}
	for _, col := range idCols {
		if col.Name == t.KeyField || col.Name == t.ValueField {
			log.Printf("warning: unpivot key_field/value_field %q collides with an identity column; output will contain duplicate column names", col.Name)
		}
	}
}
