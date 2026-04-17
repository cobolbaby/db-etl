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
			return fmt.Errorf("dst_pk is required for merge mode")
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

func watermarkSourceParts(source *config.SourceConfig) (string, string, error) {
	if source == nil {
		return "", "", fmt.Errorf("source config is required for watermark")
	}

	if table := strings.TrimSpace(source.Table); table != "" {
		return splitQualifiedName(table)
	}

	if strings.TrimSpace(source.SQL) != "" {
		return splitQualifiedName(strings.TrimSpace(source.SQLName))
	}

	return "", "", fmt.Errorf("source identity is required for watermark")
}

func watermarkTargetParts(target *config.TargetConfig) (string, string, error) {
	if target == nil {
		return "", "", fmt.Errorf("target config is required for watermark")
	}

	return splitQualifiedName(target.Table)
}
