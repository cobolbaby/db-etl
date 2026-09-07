package pipeline

import (
	"context"
	"db-etl/config"
	"db-etl/reader"
	"db-etl/transform"
	"db-etl/writer"
)

// RunPipeline 串起 reader -> transform -> writer 三级流水线，各级独立 goroutine
// 通过 channel 衔接（rowChan / batchChan），下游慢则上游阻塞形成背压。
// 出错传播：reader 出错 -> cancel(ctx) -> transform/writer 感知 ctx.Done 退出并回滚。
func RunPipeline(ctx context.Context, source *config.SourceConfig, r reader.Reader, t transform.Transformer, w writer.BatchWriter) error {
	// 派生可取消的子 ctx：reader 出错时会 cancel，令 writer 正在进行的事务以
	// context.Canceled 中止回滚，避免提交被截断的部分数据。
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	rowChan := r.ReadBatch(ctx, cancel)
	batchChan := make(chan reader.Batch, 4)

	// transform 固定单协程，以保证批次顺序不被打乱：
	//   1) 不分批提交：最后只求一次水位最大值，有序无序都无所谓；
	//   2) 分批提交：逐块推进水位并用严格 `>` 续传，多协程并行会打乱顺序而丢数。示例：
	//      reader 按序产出 A(id=1..100)、B(id=101..200)，若两协程并行使 B 先转换完，
	//      writer 先提交 B 并把水位推到 200；此时进程崩溃，A 尚未提交。重跑时续传条件
	//      `id > 200` 会跳过 A(1..100)，这批数据被永久漏掉。
	go func() {
		defer close(batchChan)
		for batch := range rowChan {
			select {
			case batchChan <- t.Transform(batch):
			case <-ctx.Done():
				return
			}
		}
	}()

	werr := w.WriteBatch(ctx, source, batchChan)

	// reader 错误优先：它是根因，会通过 cancel 触发 writer 的 context.Canceled，
	// 直接返回后者会掩盖真实原因。
	if rerr := r.Err(); rerr != nil {
		return rerr
	}
	return werr
}
