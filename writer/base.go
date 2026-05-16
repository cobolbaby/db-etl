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
// Exactly one of (Database+Schema+Table) or RawSQL is non-empty.
type wmSource struct {
	Database string
	Schema   string
	Table    string
	RawSQL   string
}

func sourceIdentity(source *config.SourceConfig) (wmSource, error) {
	if source == nil {
		return wmSource{}, fmt.Errorf("source config is required for watermark")
	}

	if table := strings.TrimSpace(source.Table); table != "" {
		parts, err := splitQualifiedName(table)
		if err != nil {
			return wmSource{}, err
		}
		if parts.Database != "" && source.DBType != config.DBTypeMSSQL {
			return wmSource{}, fmt.Errorf("source table %q uses db.schema.table, which is only supported for mssql sources", table)
		}
		return wmSource{Database: parts.Database, Schema: parts.Schema, Table: parts.Table}, nil
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

	parts, err := splitQualifiedName(target.Table)
	if err != nil {
		return wmTarget{}, err
	}
	if parts.Database != "" {
		return wmTarget{}, fmt.Errorf("target table must be table or schema.table")
	}

	return wmTarget{Schema: parts.Schema, Table: parts.Table}, nil
}

type qualifiedName struct {
	Database string
	Schema   string
	Table    string
}

func splitQualifiedName(name string) (qualifiedName, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return qualifiedName{}, fmt.Errorf("table name is required for watermark")
	}

	parts := strings.Split(trimmed, ".")
	switch len(parts) {
	case 1:
		return qualifiedName{Table: parts[0]}, nil
	case 2:
		return qualifiedName{Schema: parts[0], Table: parts[1]}, nil
	case 3:
		return qualifiedName{Database: parts[0], Schema: parts[1], Table: parts[2]}, nil
	default:
		return qualifiedName{}, fmt.Errorf("table name must be table, schema.table, or db.schema.table")
	}
}
