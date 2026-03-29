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

	log.Printf("TaskName: %s", cfg.TaskName)

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

	workers := runtime.NumCPU()
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

		srcDB, ok := dbRegistry[src.DB]
		if !ok {
			return fmt.Errorf("source db not found: %s", src.DB)
		}

		dstDB, ok := dbRegistry[task.Downstream.DB]
		if !ok {
			return fmt.Errorf("downstream db not found: %s", task.Downstream.DB)
		}

		log.Printf("Source DB: %s", src.DB)
		log.Printf("Target DB: %s", task.Downstream.DB)

		// -----------------------------
		// Reader
		// -----------------------------

		r := reader.NewReader(
			srcDB.Type,
			srcDB.DSN(),
			src.SQL,
			src.Table,
			cfg.BatchSize,
		)

		// -----------------------------
		// Writer
		// -----------------------------

		w := writer.NewWriter(
			dstDB.Type,
			dstDB.DSN(),
			task.Downstream.Table,
			task.Downstream.Mode,
		)

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
			src.DB,
			task.Downstream.DB)
	}

	return nil
}
