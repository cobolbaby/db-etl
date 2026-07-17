package pipeline

import (
	"context"
	"db-etl/config"
	"db-etl/reader"
	"db-etl/transform"
	"db-etl/writer"
	"runtime"
	"sync"
)

func RunPipeline(source *config.SourceConfig, r reader.Reader, t transform.Transformer, w writer.BatchWriter) error {
	// reader 在独立 goroutine 中异步抽取；若其出错会 cancel(ctx)，
	// 令 writer 正在进行的事务以 context.Canceled 中止并回滚，
	// 避免 full/copy 模式下提交被截断的部分数据。
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rowChan := r.ReadBatch(ctx, cancel)
	csvChan := make(chan transform.CSVBatch, 4)

	var wg sync.WaitGroup
	workers := min(runtime.NumCPU(), 2) // 4 is an empirical value, can be tuned
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range rowChan {
				select {
				case csvChan <- t.Transform(batch):
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(csvChan)
	}()

	werr := w.WriteBatch(ctx, source, csvChan)

	// reader 错误优先：它是根因，且会通过 cancel 触发 writer 的 context.Canceled。
	if rerr := r.Err(); rerr != nil {
		return rerr
	}
	return werr
}
