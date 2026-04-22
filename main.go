package main

import (
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

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
				if err := runTask(task, dbRegistry); err != nil {
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

func runTask(task config.TaskConfig, dbRegistry map[string]config.DBConfig) error {

	for _, src := range task.Sources {

		srcDB, ok := dbRegistry[src.DBName]
		if !ok {
			return fmt.Errorf("source db not found: %s", src.DBName)
		}

		dstDB, ok := dbRegistry[task.Target.DBName]
		if !ok {
			return fmt.Errorf("target db not found: %s", task.Target.DBName)
		}

		log.Printf("Source DB: %s", src.DBName)
		log.Printf("Target DB: %s", task.Target.DBName)

		// -----------------------------
		// Writer
		// -----------------------------

		w := writer.NewWriter(dstDB, task.Target, task.Name)

		// 同步方式定义在 Writer 端，但又会影响 Reader 端的抽取逻辑（增量抽取需要从目标端获取上次抽取的 Watermark 位置），
		// 所以在这里把 Mode 同步到 SourceConfig 里，Reader 和 Writer 都可以访问到
		src.Mode = task.Target.Mode

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

		r := reader.NewReader(srcDB, src)

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

		startedAt := time.Now()
		log.Printf(
			"pipeline start %s -> %s (%s)",
			src.DBName,
			task.Target.DBName,
			task.Target.Table,
		)

		err := pipeline.RunPipeline(src, r, t, w)
		if err != nil {
			log.Printf(
				"pipeline failed %s -> %s (%s) cost=%s err=%v",
				src.DBName,
				task.Target.DBName,
				task.Target.Table,
				time.Since(startedAt).Round(time.Millisecond),
				err,
			)
			return err
		}

		log.Printf(
			"pipeline finished %s -> %s (%s) cost=%s",
			src.DBName,
			task.Target.DBName,
			task.Target.Table,
			time.Since(startedAt).Round(time.Millisecond),
		)
	}

	return nil
}
