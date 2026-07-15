package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"sync"
	"time"

	"db-etl/config"
	"db-etl/pipeline"
	"db-etl/reader"
	"db-etl/transform"
	"db-etl/util"
	"db-etl/writer"
)

// Populated at build time via -ldflags.
var (
	Version   = "dev"
	Commit    = "unknown"
	BuildTime = "unknown"
)

func main() {

	configPath := flag.String("config", "config.yaml", "path to config file")
	showVersion := flag.Bool("version", false, "print version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("db-etl %s (commit %s, built %s, %s/%s)\n",
			Version, Commit, BuildTime, runtime.GOOS, runtime.GOARCH)
		return
	}

	cfg, err := config.LoadConfig(*configPath)
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
	// 2. 确定 Task 列表
	// --------------------------------

	tasks := cfg.Tasks

	if cfg.MetaDB != "" {
		// 从数据库加载任务列表，job_name 取自 config.yaml 的 name 字段
		metaDB, ok := dbRegistry[cfg.MetaDB]
		if !ok {
			log.Fatalf("meta_db %q not found in databases config", cfg.MetaDB)
		}

		dbTasks, err := config.LoadTasksFromDB(context.Background(), metaDB, cfg.Name, dbRegistry)
		if err != nil {
			log.Fatalf("load tasks from db failed: %v", err)
		}
		log.Printf("loaded %d task(s) from manager.job_data_sync for job_name=%q", len(dbTasks), cfg.Name)
		tasks = dbTasks

		// tasks = append(tasks, dbTasks...)
		// TODO: 理论上跑了一遍的任务，任务状态都会更新到数据库中，所以加载的任务列表要进行一次去重，否则可能会有重复。
		// 需要 review 一下 getWatermark 方法，获取哪个字段可能作为唯一标识符，然后与配置文件中的任务进行匹配去重。
	}

	if len(tasks) == 0 {
		log.Fatal("no tasks to run")
	}

	// --------------------------------
	// 3. Task Channel
	// --------------------------------

	taskCh := make(chan config.TaskConfig, len(tasks))
	for _, t := range tasks {
		taskCh <- t
	}
	close(taskCh)

	// --------------------------------
	// 3. Worker Pool
	// --------------------------------

	retryCfg := util.DefaultRetryConfig()
	if cfg.Retry != nil {
		if cfg.Retry.MaxAttempts > 0 {
			retryCfg.MaxAttempts = cfg.Retry.MaxAttempts
		}
		if cfg.Retry.DelaySeconds > 0 {
			retryCfg.Delay = time.Duration(cfg.Retry.DelaySeconds) * time.Second
		}
		if cfg.Retry.MaxDelaySeconds > 0 {
			retryCfg.MaxDelay = time.Duration(cfg.Retry.MaxDelaySeconds) * time.Second
		}
	}

	workers := min(runtime.NumCPU(), 4) // 4 is an empirical value, can be tuned
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskCh {
				// log.Printf("[Worker %d] start task", workerID)
				// 如果 TaskConfig 里没有 Name，就用 Config 的 Name 作为默认值
				if task.Name == "" {
					task.Name = cfg.Name
				}
				if err := runTask(task, dbRegistry, retryCfg); err != nil {
					log.Printf("task failed: %v", err)
					if cfg.ErrorPolicy == "abort" {
						log.Fatal(err)
					}
				}
				// log.Printf("[Worker %d] finish task", workerID)
			}
		}(i)
	}

	wg.Wait()
	log.Println("All tasks finished")

}

func runTask(task config.TaskConfig, dbRegistry map[string]config.DBConfig, retryCfg util.RetryConfig) error {

	for _, src := range task.Sources {

		srcDB, ok := dbRegistry[src.DBName]
		if !ok {
			return fmt.Errorf("source db not found: %s", src.DBName)
		}
		src.DBType = srcDB.Type
		if err := config.ValidateSourceTableName(src.Table, src.DBType); err != nil {
			return err
		}

		dstDB, ok := dbRegistry[task.Target.DBName]
		if !ok {
			return fmt.Errorf("target db not found: %s", task.Target.DBName)
		}

		label := fmt.Sprintf("%s → %s (%s)", src.DBName, task.Target.DBName, task.Target.Table)
		err := util.Retry(label, retryCfg, func() error {
			return runPipeline(src, srcDB, dstDB, task)
		})
		if err != nil {
			log.Printf("pipeline failed %s after retries: %v", label, err)
			continue
		}
	}

	return nil
}

func runPipeline(src *config.SourceConfig, srcDB config.DBConfig, dstDB config.DBConfig, task config.TaskConfig) error {

	// mc := metrics.Default()
	// pm := mc.NewPipelineMetrics(src.DBName, task.Target.DBName, task.Target.Table, string(task.Target.Mode))

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
		log.Printf("incr extraction mode, target table: %s, field: %s, point: %s", task.Target.Table, src.IncrField, src.IncrPoint)
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
	// mc.Finish(pm, err)

	if err != nil {
		return fmt.Errorf(
			"pipeline failed %s -> %s (%s) cost=%s: %w",
			src.DBName,
			task.Target.DBName,
			task.Target.Table,
			time.Since(startedAt).Round(time.Millisecond),
			err,
		)

	}

	log.Printf(
		"pipeline finished %s -> %s (%s) cost=%s",
		src.DBName,
		task.Target.DBName,
		task.Target.Table,
		time.Since(startedAt).Round(time.Millisecond),
	)
	return nil
}
