package transform

import (
	"db-etl/reader"
	"db-etl/util"
	"log"
	"sync"
)

type CSVBatch struct {
	Columns []string
	Rows    [][]string
}

type Transformer interface {
	// RowBatch -> CSVBatch
	Transform(batch reader.RowBatch) CSVBatch
}

// CSVTransformer 是链式转换步骤：在已序列化的 CSVBatch 上做进一步重塑（CSVBatch -> CSVBatch）。
// 基座 DefaultTransformer 完成 RowBatch -> CSVBatch 的类型序列化后，
// 额外配置的转换步骤按顺序在其结果上叠加应用。
type CSVTransformer interface {
	TransformCSV(batch CSVBatch) CSVBatch
}

type DefaultTransformer struct {
	Handlers []reader.ColHandler
}

func (t *DefaultTransformer) Transform(batch reader.RowBatch) CSVBatch {
	res := make([][]string, len(batch.Rows))
	for i, row := range batch.Rows {
		rec := make([]string, len(row))
		for j, v := range row {
			rec[j] = t.Handlers[j](v)
		}
		res[i] = rec
	}
	return CSVBatch{Columns: batch.Columns, Rows: res}
}

// ChainTransformer 以 DefaultTransformer 为基座，按顺序叠加若干 CSVTransformer 步骤。
type ChainTransformer struct {
	Base  Transformer
	Steps []CSVTransformer
}

func (t *ChainTransformer) Transform(batch reader.RowBatch) CSVBatch {
	out := t.Base.Transform(batch)
	for _, step := range t.Steps {
		out = step.TransformCSV(out)
	}
	return out
}

// UnpivotTransformer 将宽表列转行：把 Columns 中列出的源列展开成两列（Key/Value）的多行，
// 其余列作为标识列在每个展开行中重复保留。它作用于已序列化的 CSVBatch，
// 源端只需抽取宽表（读取量小），避免在源端 SQL 中重复标识列导致源库→ETL 网络传输量成倍放大。
type UnpivotTransformer struct {
	// KeyField 输出中承载“列标签”的列名。
	KeyField string
	// ValueField 输出中承载“列值”的列名。
	ValueField string
	// Columns 为「源列名 -> 写入 KeyField 的标签」映射；未命中的列作为标识列保留。
	Columns map[string]string
	// DropNull 为 true 时跳过源列值为 NULL 的展开行（NULL 已被序列化为 util.NullSentinel）。
	DropNull bool

	// warnOnce 保证列布局异常（配置列缺失、输出列重名）只在首批告警一次，避免日志刷屏。
	warnOnce sync.Once
}

func (t *UnpivotTransformer) TransformCSV(batch CSVBatch) CSVBatch {
	// 按当前批次的列结构解析标识列与待展开列。列结构在整个任务中稳定，
	// 每批重算成本极低（O(列数)），且避免在并发 worker 间共享可变状态。
	type pivotCol struct {
		idx   int
		label string
	}
	idIdx := make([]int, 0, len(batch.Columns))
	idNames := make([]string, 0, len(batch.Columns))
	pivots := make([]pivotCol, 0, len(t.Columns))
	resolved := make(map[string]struct{}, len(t.Columns))
	for i, name := range batch.Columns {
		if label, ok := t.Columns[name]; ok {
			pivots = append(pivots, pivotCol{idx: i, label: label})
			resolved[name] = struct{}{}
			continue
		}
		idIdx = append(idIdx, i)
		idNames = append(idNames, name)
	}

	t.warnOnce.Do(func() { t.checkLayout(idNames, resolved) })

	outCols := make([]string, 0, len(idNames)+2)
	outCols = append(outCols, idNames...)
	outCols = append(outCols, t.KeyField, t.ValueField)

	outRows := make([][]string, 0, len(batch.Rows)*len(pivots))
	for _, row := range batch.Rows {
		// 标识列每输入行只取一次，供该行的多个展开行复用。
		idVals := make([]string, len(idIdx))
		for k, ci := range idIdx {
			idVals[k] = row[ci]
		}
		for _, p := range pivots {
			value := row[p.idx]
			if t.DropNull && value == util.NullSentinel {
				continue
			}
			rec := make([]string, 0, len(idVals)+2)
			rec = append(rec, idVals...)
			rec = append(rec, p.label, value)
			outRows = append(outRows, rec)
		}
	}

	return CSVBatch{Columns: outCols, Rows: outRows}
}

// checkLayout 在首批数据上核对列布局，对易导致静默错误数据的配置问题发出告警：
//   - 配置的待展开列有部分/全部在实际结果中不存在（列名拼写或大小写不符）；
//   - key_field / value_field 与某个标识列同名，导致输出列重名、写入错列。
func (t *UnpivotTransformer) checkLayout(idNames []string, resolved map[string]struct{}) {
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
	for _, name := range idNames {
		if name == t.KeyField || name == t.ValueField {
			log.Printf("warning: unpivot key_field/value_field %q collides with an identity column; output will contain duplicate column names", name)
		}
	}
}
