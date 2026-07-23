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
	// 1. 构建 DB Resolver（conn_id 优先，name 回退）
	// --------------------------------

	resolver := config.NewDBResolver(cfg.Databases)

	// --------------------------------
	// 2. 确定 Task 列表
	// --------------------------------

	tasks := cfg.Tasks

	if cfg.MetaDB != "" {
		// 从数据库加载任务列表，job_name 取自 config.yaml 的 name 字段
		metaDB, ok := resolver.Resolve(cfg.MetaDB, cfg.MetaDB)
		if !ok {
			log.Fatalf("meta_db %q not found in databases config", cfg.MetaDB)
		}

		dbTasks, err := config.LoadTasksFromDB(context.Background(), metaDB, cfg.Name, resolver)
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
				if err := runTask(task, resolver, retryCfg); err != nil {
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

func runTask(task config.TaskConfig, resolver config.DBResolver, retryCfg util.RetryConfig) error {

	for _, src := range task.Sources {

		srcDB, ok := resolver.Resolve(src.ConnID, src.ConnName)
		if !ok {
			return fmt.Errorf("source db not found (conn_id=%q conn_name=%q)", src.ConnID, src.ConnName)
		}
		src.DBType = srcDB.Type
		if err := config.ValidateSourceTableName(src.Table, src.DBType); err != nil {
			return err
		}

		dstDB, ok := resolver.Resolve(task.Target.ConnID, task.Target.ConnName)
		if !ok {
			return fmt.Errorf("target db not found (conn_id=%q conn_name=%q)", task.Target.ConnID, task.Target.ConnName)
		}

		label := fmt.Sprintf("%s (%s) → %s (%s)", srcDB.Name, src.Table, dstDB.Name, task.Target.Table)
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
	// pm := mc.NewPipelineMetrics(src.ConnName, task.Target.ConnName, task.Target.Table, string(task.Target.Mode))

	// -----------------------------
	// Writer
	// -----------------------------

	w, err := writer.NewWriter(dstDB, task.Target, task.Name)
	if err != nil {
		return fmt.Errorf("create writer: %w", err)
	}
	// 每次 pipeline（含重试的每一轮）结束都关闭连接，避免重试重建连接时泄漏。
	defer w.Close()

	// 同步方式定义在 Writer 端，但又会影响 Reader 端的抽取逻辑（增量抽取需要从目标端获取上次抽取的 Watermark 位置），
	// 所以在这里把 Mode 同步到 SourceConfig 里，Reader 和 Writer 都可以访问到
	src.Mode = task.Target.Mode

	// 增量抽取需要知道上次抽取的 Watermark 位置。
	// IncrPoint 非空说明启动时已从 job_data_sync（db_loader）或配置文件加载到水位，直接复用，省一次查询；
	// 仅当为空（首次运行、尚无水位）时才查库，走 job_data_sync → MAX(incr_field) → 默认值 的兜底链。
	// 仅增量模式（append/merge）需要水位；全量模式（full/initial）为一次性覆盖/回填，不做水位追踪。
	if (src.Mode == config.ModeTypeAppend || src.Mode == config.ModeTypeMerge) && src.IncrField != "" {

		if src.IncrPoint == "" {
			incrPoint, err := w.GetWatermark(src)
			if err != nil {
				return fmt.Errorf("failed to get incr point: %v", err)
			}
			src.IncrPoint = incrPoint
		}
		log.Printf("incr extraction mode, target table: %s, field: %s, point: %s", task.Target.Table, src.IncrField, src.IncrPoint)
	}

	// -----------------------------
	// Reader
	// -----------------------------

	r, err := reader.NewReader(srcDB, src)
	if err != nil {
		return fmt.Errorf("create reader: %w", err)
	}
	defer r.Close()

	// -----------------------------
	// Transformer
	// -----------------------------

	handlers, err := r.GetColumnHandlers()
	if err != nil {
		return fmt.Errorf("get column handlers: %w", err)
	}
	t := &transform.DefaultTransformer{
		Handlers: handlers,
	}

	// -----------------------------
	// Pipeline
	// -----------------------------

	startedAt := time.Now()
	log.Printf(
		"pipeline start %s (%s) -> %s (%s)",
		srcDB.Name,
		src.Table,
		dstDB.Name,
		task.Target.Table,
	)

	err = pipeline.RunPipeline(src, r, t, w)
	// mc.Finish(pm, err)

	if err != nil {
		return fmt.Errorf(
			"pipeline failed %s (%s) -> %s (%s) cost=%s: %w",
			srcDB.Name,
			src.Table,
			dstDB.Name,
			task.Target.Table,
			time.Since(startedAt).Round(time.Millisecond),
			err,
		)

	}

	log.Printf(
		"pipeline finished %s (%s) -> %s (%s) cost=%s",
		srcDB.Name,
		src.Table,
		dstDB.Name,
		task.Target.Table,
		time.Since(startedAt).Round(time.Millisecond),
	)
	return nil
}
