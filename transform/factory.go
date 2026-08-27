package transform

import (
	"db-etl/config"
)

// NewTransformer 根据 task 的 transform 链构造 Transformer。
// 列元数据随 Batch 同行，故此处无需预先获知源端列结构；无转换配置时返回原样透传的实现。
func NewTransformer(cfgs []*config.TransformConfig) Transformer {
	if len(cfgs) == 0 {
		return passthrough{}
	}

	steps := make([]Transformer, 0, len(cfgs))
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
		return passthrough{}
	}

	return &chainTransformer{steps: steps}
}
