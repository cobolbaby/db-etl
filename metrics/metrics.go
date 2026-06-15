package metrics

import (
	"log"
	"sync"
	"sync/atomic"
	"time"
)

// PipelineMetrics tracks statistics for a single pipeline execution.
type PipelineMetrics struct {
	SourceDB   string
	TargetDB   string
	Table      string
	Mode       string
	StartedAt  time.Time
	FinishedAt time.Time

	RowsRead    atomic.Int64
	RowsWritten atomic.Int64
	BatchesRead atomic.Int64
	BytesWritten atomic.Int64
	Err         error
}

// Duration returns the pipeline execution duration.
func (m *PipelineMetrics) Duration() time.Duration {
	if m.FinishedAt.IsZero() {
		return time.Since(m.StartedAt)
	}
	return m.FinishedAt.Sub(m.StartedAt)
}

// Reporter is the interface for metrics reporting backends.
type Reporter interface {
	Report(m *PipelineMetrics)
}

// LogReporter prints metrics to the standard logger.
type LogReporter struct{}

func (r *LogReporter) Report(m *PipelineMetrics) {
	status := "success"
	if m.Err != nil {
		status = "failed"
	}
	log.Printf("[metrics] %s→%s (%s) mode=%s status=%s rows_read=%d rows_written=%d duration=%s",
		m.SourceDB, m.TargetDB, m.Table, m.Mode, status,
		m.RowsRead.Load(), m.RowsWritten.Load(),
		m.Duration().Round(time.Millisecond),
	)
}

// Collector holds a list of reporters and accumulated metrics.
type Collector struct {
	mu        sync.Mutex
	reporters []Reporter
	history   []*PipelineMetrics
}

// Global default collector.
var defaultCollector = &Collector{
	reporters: []Reporter{&LogReporter{}},
}

// Default returns the global metrics collector.
func Default() *Collector {
	return defaultCollector
}

// AddReporter registers an additional reporter.
func (c *Collector) AddReporter(r Reporter) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reporters = append(c.reporters, r)
}

// NewPipelineMetrics creates and starts tracking a new pipeline.
func (c *Collector) NewPipelineMetrics(sourceDB, targetDB, table, mode string) *PipelineMetrics {
	return &PipelineMetrics{
		SourceDB:  sourceDB,
		TargetDB:  targetDB,
		Table:     table,
		Mode:      mode,
		StartedAt: time.Now(),
	}
}

// Finish finalizes the metrics and sends to all reporters.
func (c *Collector) Finish(m *PipelineMetrics, err error) {
	m.FinishedAt = time.Now()
	m.Err = err

	c.mu.Lock()
	c.history = append(c.history, m)
	reporters := make([]Reporter, len(c.reporters))
	copy(reporters, c.reporters)
	c.mu.Unlock()

	for _, r := range reporters {
		r.Report(m)
	}
}

// Summary returns all collected metrics (for programmatic access / testing).
func (c *Collector) Summary() []*PipelineMetrics {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*PipelineMetrics, len(c.history))
	copy(out, c.history)
	return out
}
