package main

import (
	"fmt"
	"log"
	"runtime"
	"sync"

	"db-etl/config"
	"db-etl/pipeline"
	"db-etl/reader"
	"db-etl/transform"
	"db-etl/writer"
)

func main() {

	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatal(err)
	}

	// --------------------------------
	// 1. 构建 DB Registry
	// --------------------------------

	dbRegistry := map[string]config.DBConfig{}
	for _, db := range cfg.Databases {
		dbRegistry[db.Name] = db
	}

	// --------------------------------
	// 2. Task Channel
	// --------------------------------

	taskCh := make(chan config.TaskConfig, len(cfg.Tasks))
	for _, t := range cfg.Tasks {
		taskCh <- t
	}
	close(taskCh)

	// --------------------------------
	// 3. Worker Pool
	// --------------------------------

	workers := min(runtime.NumCPU(), 4) // 4 is an empirical value, can be tuned
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskCh {
				log.Printf("[Worker %d] start task", workerID)
				if err := runTask(cfg, task, dbRegistry); err != nil {
					log.Printf("task failed: %v", err)
					if cfg.ErrorPolicy == "abort" {
						log.Fatal(err)
					}
				}
				log.Printf("[Worker %d] finish task", workerID)
			}
		}(i)
	}

	wg.Wait()
	log.Println("All tasks finished")

}

func runTask(cfg *config.Config, task config.TaskConfig, dbRegistry map[string]config.DBConfig) error {

	for _, src := range task.Sources {

		srcDB, ok := dbRegistry[src.Name]
		if !ok {
			return fmt.Errorf("source db not found: %s", src.Name)
		}

		dstDB, ok := dbRegistry[task.Downstream.Name]
		if !ok {
			return fmt.Errorf("downstream db not found: %s", task.Downstream.Name)
		}

		log.Printf("Source DB: %s", src.Name)
		log.Printf("Target DB: %s", task.Downstream.Name)

		// -----------------------------
		// Writer
		// -----------------------------

		w := writer.NewWriter(dstDB, task.Downstream)

		// 同步方式定义在 Writer 端，但又会影响 Reader 端的抽取逻辑（增量抽取需要从目标端获取上次抽取的 Watermark 位置），
		// 所以在这里把 Mode 同步到 SourceConfig 里，Reader 和 Writer 都可以访问到
		src.Mode = task.Downstream.Mode

		// 增量抽取需要知道上次抽取的 Watermark 位置，这个位置存在目标数据库里
		if src.Mode != config.ModeTypeFull && src.IncrField != "" {

			incrPoint, err := w.GetWatermark(src)
			if err != nil {
				return fmt.Errorf("failed to get incr point: %v", err)
			}
			src.IncrPoint = incrPoint
			log.Printf("Incr extraction mode, field: %s, point: %s", src.IncrField, src.IncrPoint)
		}

		// -----------------------------
		// Reader
		// -----------------------------

		r := reader.NewReader(srcDB, src, cfg.BatchSize)

		// -----------------------------
		// Transformer
		// -----------------------------

		handlers := r.GetColumnHandlers()
		t := &transform.DefaultTransformer{
			Handlers: handlers,
		}

		// -----------------------------
		// Pipeline
		// -----------------------------

		err := pipeline.RunPipeline(r, t, w)
		if err != nil {
			return err
		}

		log.Printf("finished source %s -> %s",
			src.Name,
			task.Downstream.Name)
	}

	return nil
}
