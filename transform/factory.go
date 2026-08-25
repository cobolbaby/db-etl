package transform

import (
	"db-etl/config"
	"db-etl/reader"
)

// NewTransformer 根据 task 的 transform 链构造 Transformer。
// 以列元数据为基座，transform 列表中的每个步骤按顺序在其后叠加。
// columns 由 reader.GetColumnMeta 提供。
func NewTransformer(cfgs []*config.TransformConfig, columns []reader.ColumnMeta) Transformer {
	base := &baseTransformer{columns: columns}
	if len(cfgs) == 0 {
		return base
	}

	steps := make([]Step, 0, len(cfgs))
	for _, cfg := range cfgs {
		if cfg == nil {
			continue
		}
		if cfg.Unpivot != nil {
			steps = append(steps, &UnpivotTransformer{
				KeyField:   cfg.Unpivot.KeyField,
				ValueField: cfg.Unpivot.ValueField,
				Columns:    cfg.Unpivot.Columns,
				DropNull:   cfg.Unpivot.DropNull,
			})
		}
	}

	if len(steps) == 0 {
		return base
	}

	return &chainTransformer{base: base, steps: steps}
}
