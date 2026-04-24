package writer

import (
	"db-etl/config"
	"db-etl/transform"
	"fmt"
	"strings"
)

type writerDialect interface {
	writeCopy(in <-chan transform.CSVBatch, table string) error
	writeMerge(in <-chan transform.CSVBatch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error
	getWatermark(target *config.TargetConfig, source *config.SourceConfig, jobName string) (string, error)
}

type BaseWriter struct {
	Target  *config.TargetConfig
	JobName string
	dialect writerDialect
}

func (w *BaseWriter) WriteBatch(source *config.SourceConfig, in <-chan transform.CSVBatch) error {
	if w.Target == nil {
		return fmt.Errorf("target config is required")
	}

	switch w.Target.Mode {
	case config.ModeTypeCopy:
		return w.dialect.writeCopy(in, w.Target.Table)
	case config.ModeTypeMerge:
		if w.Target.PK == "" {
			return fmt.Errorf("pk is required for merge mode")
		}
		return w.dialect.writeMerge(in, w.Target, source, w.JobName)
	default:
		return fmt.Errorf("unsupported mode: %s", w.Target.Mode)
	}
}

func (w *BaseWriter) GetWatermark(source *config.SourceConfig) (string, error) {
	return w.dialect.getWatermark(w.Target, source, w.JobName)
}

func watermarkJobName(jobName string) string {
	return strings.TrimSpace(jobName)
}

// wmSource holds the resolved identity of a source for watermark queries.
// Exactly one of (Schema+Table) or RawSQL is non-empty.
type wmSource struct {
	Schema string
	Table  string
	RawSQL string
}

func sourceIdentity(source *config.SourceConfig) (wmSource, error) {
	if source == nil {
		return wmSource{}, fmt.Errorf("source config is required for watermark")
	}

	if table := strings.TrimSpace(source.Table); table != "" {
		schema, tbl, err := splitQualifiedName(table)
		if err != nil {
			return wmSource{}, err
		}
		return wmSource{Schema: schema, Table: tbl}, nil
	}

	if sql := strings.TrimSpace(source.SQL); sql != "" {
		return wmSource{RawSQL: sql}, nil
	}

	return wmSource{}, fmt.Errorf("source identity is required for watermark")
}

// wmTarget holds the resolved identity of a destination table for watermark queries.
type wmTarget struct {
	Schema string
	Table  string
}

func targetIdentity(target *config.TargetConfig) (wmTarget, error) {
	if target == nil {
		return wmTarget{}, fmt.Errorf("target config is required for watermark")
	}

	schema, tbl, err := splitQualifiedName(target.Table)
	if err != nil {
		return wmTarget{}, err
	}

	return wmTarget{Schema: schema, Table: tbl}, nil
}
