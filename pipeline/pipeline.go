package pipeline

import (
	"db-etl/config"
	"db-etl/reader"
	"db-etl/transform"
	"db-etl/writer"
	"runtime"
	"sync"
)

func RunPipeline(source *config.SourceConfig, r reader.Reader, t transform.Transformer, w writer.BatchWriter) error {
	rowChan := r.ReadBatch()
	csvChan := make(chan transform.CSVBatch, 8)

	var wg sync.WaitGroup
	workers := min(runtime.NumCPU(), 2) // 4 is an empirical value, can be tuned
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range rowChan {
				csvChan <- t.Transform(batch)
			}
		}()
	}

	go func() {
		wg.Wait()
		close(csvChan)
	}()

	return w.WriteBatch(source, csvChan)
}
