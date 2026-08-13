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

// Transformer 是转换链的统一入口：把 reader 抽取的 RowBatch 转换为字符串 CSVBatch。
// 有两个实现：DefaultTransformer 只做基础的类型序列化；ChainTransformer 在序列化
// 结果之上再按顺序叠加额外的转换步骤（如列转行）。pipeline 对每个批次只调用一次 Transform。
type Transformer interface {
	Transform(batch reader.RowBatch) CSVBatch
}

// CSVTransformer 是链上的转换步骤：在已序列化的 CSVBatch 上做进一步重塑（CSVBatch -> CSVBatch）。
// 因输入输出同构而可按顺序自由串接，叠加在 DefaultTransformer 的序列化结果之后。
type CSVTransformer interface {
	Transform(batch CSVBatch) CSVBatch
}

// ChainTransformer 以 DefaultTransformer 为基座，按顺序叠加若干 CSVTransformer 步骤。
// 无额外步骤时行为等价于 DefaultTransformer（仅做序列化透传）。
type ChainTransformer struct {
	Base  *DefaultTransformer
	Steps []CSVTransformer
}

func (t *ChainTransformer) Transform(batch reader.RowBatch) CSVBatch {
	out := t.Base.Transform(batch)
	for _, step := range t.Steps {
		out = step.Transform(out)
	}
	return out
}

// DefaultTransformer 是所有任务共用的基座：把 RowBatch 按列类型逐列序列化为字符串 CSVBatch。
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

// UnpivotTransformer 将宽表列转行：把 Columns 中列出的源列展开成 KeyField/ValueField 两列的多行，
// 其余列作为标识列在每个展开行中重复保留。它作用于已序列化的 CSVBatch。
// 把 unpivot 放在转换阶段（而非源端 SQL）可让源端只抽宽表，避免标识列在源库→ETL 链路上
// 随展开行数成倍重复传输。
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

func (t *UnpivotTransformer) Transform(batch CSVBatch) CSVBatch {
	// 每批根据当前列名重新解析标识列与待展开列：转换器实例在多个 worker 间共享，
	// 将解析结果保留为局部变量（而非缓存到字段）可避免共享可变状态、天然并发安全；
	// 解析成本为 O(列数)，相对每批的行处理可忽略。
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
