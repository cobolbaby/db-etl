package pipeline

import (
	"db-etl/reader"
	"db-etl/transform"
	"db-etl/writer"
	"runtime"
	"sync"
)

func RunPipeline(r reader.Reader, t transform.Transformer, w writer.Writer) error {
	rowChan := r.ReadBatch()
	csvChan := make(chan transform.CSVBatch, 8)

	var wg sync.WaitGroup
	workers := runtime.NumCPU() * 2
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

	return w.WriteBatch(csvChan)
}
