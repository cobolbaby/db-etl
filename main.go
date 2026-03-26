package main

import (
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

	tableCh := make(chan config.TableMap, len(cfg.Tables))
	for _, t := range cfg.Tables {
		tableCh <- t
	}
	close(tableCh)

	workers := runtime.NumCPU()
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for tbl := range tableCh {
				log.Printf("Migrating %s -> %s", tbl.Src, tbl.Dst)

				// 动态创建 Reader / Writer
				r := reader.NewReader(cfg.Source.Type, cfg.Source.DSN(), tbl.Src, cfg.BatchSize)
				w := writer.NewWriter(cfg.Target.Type, cfg.Target.DSN(), tbl.Dst)

				// 根据数据源类型生成 transformer
				handlers := r.GetColumnHandlers()
				tTransformer := &transform.DefaultTransformer{Handlers: handlers}

				if err := pipeline.RunPipeline(r, tTransformer, w); err != nil {
					log.Fatalf("Failed to migrate table %s: %v", tbl.Src, err)
				}

				log.Printf("Finished %s -> %s", tbl.Src, tbl.Dst)
			}
		}()
	}

	wg.Wait()
	log.Printf("All tables migrated!")
}
