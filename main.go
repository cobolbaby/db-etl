package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync"
	"time"

	"db-etl/config"
	"db-etl/hook"
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

	dbResolver := config.NewDBResolver(cfg.Databases)

	// 构建 S3 Resolver（对象存储目标按 name 引用）
	s3Resolver := config.NewS3Resolver(cfg.S3)

	// --------------------------------
	// 2. 确定 Task 列表
	// --------------------------------

	tasks := cfg.Tasks

	// managerDB 指向存放 manager.job_data_sync 的 PostgreSQL（meta_db）；
	// 未配置 meta_db 时保持零值，下游据此跳过水位回写。
	var managerDB config.DBConfig

	if cfg.MetaDB != "" {
		// 从数据库加载任务列表，job_name 取自 config.yaml 的 name 字段
		var ok bool
		managerDB, ok = dbResolver.Resolve(cfg.MetaDB, cfg.MetaDB)
		if !ok {
			log.Fatalf("meta_db %q not found in databases config", cfg.MetaDB)
		}

		dbTasks, err := config.LoadTasksFromDB(context.Background(), managerDB, cfg.Name, dbResolver)
		if err != nil {
			log.Fatalf("load tasks from db failed: %v", err)
		}
		log.Printf("loaded %d task(s) from manager.job_data_sync for job_name=%q", len(dbTasks), cfg.Name)
		tasks = dbTasks

		// tasks = append(tasks, dbTasks...)
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
	e := &etl{
		dbResolver: dbResolver,
		s3Resolver: s3Resolver,
		managerDB:  managerDB,
		retryCfg:   retryCfg,
		jobName:    cfg.Name,
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskCh {
				// log.Printf("[Worker %d] start task", workerID)
				if err := e.runTask(context.Background(), task); err != nil {
					log.Printf("task failed: %v", err)
					if cfg.ErrorPolicy == config.ErrorPolicyAbort {
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

// etl 聚合各 task/pipeline 共享的依赖，避免在 runTask/runPipeline 间层层透传。
type etl struct {
	// jobName 为 job 级默认任务名（config.yaml 的 name），task 未指定 name 时取此值。
	jobName    string
	dbResolver config.DBResolver
	s3Resolver config.S3Resolver
	managerDB  config.DBConfig
	retryCfg   util.RetryConfig
}

func (e *etl) runTask(ctx context.Context, task config.TaskConfig) error {
	if task.Name == "" {
		task.Name = e.jobName
	}

	// 执行前置 hook
	if task.Hooks != nil && len(task.Hooks.Pre) > 0 {
		if err := hook.RunPreHooks(ctx, task.Hooks.Pre, e.dbResolver); err != nil {
			return fmt.Errorf("run pre-hooks failed: %w", err)
		}
	}

	// 执行数据同步
	var lastErr error
	for _, src := range task.Sources {

		srcDB, ok := e.dbResolver.Resolve(src.ConnID, src.ConnName)
		if !ok {
			return fmt.Errorf("source db not found (conn_id=%q conn_name=%q)", src.ConnID, src.ConnName)
		}

		dstName := e.targetName(task.Target)
		label := fmt.Sprintf("%s (%s) → %s (%s)", srcDB.Name, src.Table, dstName, task.Target.Table)
		err := util.Retry(label, e.retryCfg, func() error {
			return e.runPipeline(ctx, src, srcDB, task)
		})
		if err != nil {
			log.Printf("pipeline failed %s after retries: %v", label, err)
			lastErr = err
			continue
		}
	}

	// 执行后置 hook
	if task.Hooks != nil && len(task.Hooks.Post) > 0 {
		if err := hook.RunPostHooks(ctx, task.Hooks.Post, e.dbResolver); err != nil {
			return fmt.Errorf("run post-hooks failed: %w", err)
		}
	}

	return lastErr
}

func (e *etl) runPipeline(ctx context.Context, src *config.SourceConfig, srcDB config.DBConfig, task config.TaskConfig) error {

	// mc := metrics.Default()
	// pm := mc.NewPipelineMetrics(src.ConnName, task.Target.ConnName, task.Target.Table, string(task.Target.Mode))

	dstName := e.targetName(task.Target)

	// -----------------------------
	// Writer
	// -----------------------------

	w, err := writer.NewWriter(task.Target, e.dbResolver, e.s3Resolver, e.managerDB, task.Name)
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

	columns, err := r.GetColumnMeta()
	if err != nil {
		return fmt.Errorf("get column meta: %w", err)
	}
	t := transform.NewTransformer(task.Transform, columns)

	// -----------------------------
	// Pipeline
	// -----------------------------

	startedAt := time.Now()
	log.Printf(
		"pipeline start %s (%s) -> %s (%s)",
		srcDB.Name,
		src.Table,
		dstName,
		task.Target.Table,
	)

	err = pipeline.RunPipeline(ctx, src, r, t, w)
	// mc.Finish(pm, err)

	if err != nil {
		return fmt.Errorf(
			"pipeline failed %s (%s) -> %s (%s) cost=%s: %w",
			srcDB.Name,
			src.Table,
			dstName,
			task.Target.Table,
			time.Since(startedAt).Round(time.Millisecond),
			err,
		)

	}

	log.Printf(
		"pipeline finished %s (%s) -> %s (%s) cost=%s",
		srcDB.Name,
		src.Table,
		dstName,
		task.Target.Table,
		time.Since(startedAt).Round(time.Millisecond),
	)
	return nil
}

// targetName 返回目标端用于日志的名称：对象存储目标取 s3 名，数据库目标取库名。
func (e *etl) targetName(target *config.TargetConfig) string {
	if strings.TrimSpace(target.S3) != "" {
		return target.S3
	}
	if db, ok := e.dbResolver.Resolve(target.ConnID, target.ConnName); ok {
		return db.Name
	}
	if target.ConnName != "" {
		return target.ConnName
	}
	return target.ConnID
}
