package transform

import (
	"db-etl/config"
	"db-etl/reader"
)

// NewTransformer 根据 task 的 transform 链构造 Transformer。
// 以 DefaultTransformer 为基座完成类型序列化；transform 列表中的每个步骤按顺序在其后叠加。
// 列表为空时直接返回基座，避免链式包装的额外开销。
func NewTransformer(cfgs []*config.TransformConfig, handlers []reader.ColHandler) Transformer {
	base := &DefaultTransformer{Handlers: handlers}
	if len(cfgs) == 0 {
		return base
	}

	steps := make([]CSVTransformer, 0, len(cfgs))
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

	return &ChainTransformer{Base: base, Steps: steps}
}
