package writer

import (
	"context"
	"db-etl/config"
	"db-etl/reader"
	"db-etl/transform"
	"db-etl/util"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/parquet-go/parquet-go"
)

// parquetFlushRows 控制每积攒多少行调用一次 WriteRows，平衡内存与调用开销。
const parquetFlushRows = 1024

// parquetWriterDialect 将 Batch 序列化为 parquet 并流式上传到对象存储。
//   - full/initial：覆盖写单个 <table>.parquet 对象（每次全量刷新）。
//   - append/merge：每次增量写一个以起点水位命名的对象，并把水位写回 manager.job_data_sync。
//
// managerConn 指向存放 manager.job_data_sync 的 PostgreSQL；为 nil 时跳过水位写回/读取
// （退化为无状态增量，起点由 defaultIncrPoint 兜底）。
type parquetWriterDialect struct {
	store       *s3Store
	managerConn *pgx.Conn
}

// NewParquetWriter 构建写 parquet 到对象存储的 Writer。
// s3 提供对象存储连接信息；managerDB 提供水位写回连接，为零值（未配置 meta_db）时不建连接。
func NewParquetWriter(s3 config.S3Config, managerDB config.DBConfig, target *config.TargetConfig, jobName string) (Writer, error) {
	if target == nil {
		return nil, util.NonRetryable(fmt.Errorf("target config is required for parquet writer"))
	}

	store, err := newS3Store(s3)
	if err != nil {
		return nil, err
	}

	// Database 是每个 datasource 的必填项（见 config.validateDatabases），
	// 为空即表示未配置 meta_db，此时不建连接、跳过水位读写。
	var managerConn *pgx.Conn
	if managerDB.Database != "" {
		conn, err := pgx.Connect(context.Background(), managerDB.DSN())
		if err != nil {
			return nil, fmt.Errorf("manager DB connect failed: %w", err)
		}
		managerConn = conn
	}

	base := &BaseWriter{
		Target:  target,
		JobName: jobName,
	}
	base.dialect = &parquetWriterDialect{store: store, managerConn: managerConn}

	return base, nil
}

// close 关闭 manager 连接（若有）。
func (d *parquetWriterDialect) close(ctx context.Context) error {
	if d.managerConn == nil {
		return nil
	}
	return d.managerConn.Close(ctx)
}

func (d *parquetWriterDialect) writeInitial(ctx context.Context, in <-chan transform.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	// initial 视为一次性全量：覆盖写单对象；成功后置 inuse=false，避免下次重复回填。
	if _, err := d.writeObject(ctx, in, defaultObjectKey(target), source); err != nil {
		return err
	}
	if d.managerConn != nil {
		if _, err := execDeactivateInitialJob(ctx, d.managerConn, target, source, jobName); err != nil {
			return err
		}
		log.Printf("initial parquet task done, set inuse=false (table=%s)", target.Table)
	}
	return nil
}

func (d *parquetWriterDialect) writeFull(ctx context.Context, in <-chan transform.Batch, target *config.TargetConfig) error {
	// full 全量刷新：覆盖写单对象。无水位、无 source 依赖。
	_, err := d.writeObject(ctx, in, defaultObjectKey(target), nil)
	return err
}

func (d *parquetWriterDialect) writeAppend(ctx context.Context, in <-chan transform.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	return d.writeIncremental(ctx, in, target, source, jobName)
}

func (d *parquetWriterDialect) writeMerge(ctx context.Context, in <-chan transform.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	// 对象存储不可按 PK 原地删除/更新，merge 与 append 一致：追加新对象，去重交由下游查询处理。
	log.Printf("parquet target does not support in-place merge by pk; writing incremental object for table=%s (downstream must dedupe by pk=%s)", target.Table, target.PK)
	return d.writeIncremental(ctx, in, target, source, jobName)
}

// writeIncremental 每次增量写一个新对象，并把最大水位写回 manager。
func (d *parquetWriterDialect) writeIncremental(ctx context.Context, in <-chan transform.Batch, target *config.TargetConfig, source *config.SourceConfig, jobName string) error {
	key := incrementalObjectKey(target, source)

	wm, err := d.writeObject(ctx, in, key, source)
	if err != nil {
		return err
	}

	// 无数据或未追踪到水位则不推进（保持既有 incr_point 不变）。
	if wm == "" || d.managerConn == nil || source == nil {
		return nil
	}
	return upsertWatermark(ctx, d.managerConn, wm, target, source, jobName)
}

// parquetEncoder 持有单个对象的编码状态：列映射、类型、增量水位追踪与行缓冲，
// 负责把 Batch 逐行编码写入 parquet.GenericWriter。
type parquetEncoder struct {
	columns  []reader.ColumnMeta
	colIndex []int // 输入列序 → schema 叶子列序（parquet.Group 为 map，叶子顺序由库决定）
	incrIdx  int   // 增量字段所在列下标，-1 表示无
	rb       *parquet.RowBuilder
	buf      []parquet.Row
	maxWM    string // 已见最大水位
	warned   bool   // 是否已打印过转换失败告警（避免刷屏）
}

// newParquetEncoder 依首个 batch 的列信息构造 schema 与编码器。
func newParquetEncoder(columns []reader.ColumnMeta, source *config.SourceConfig) (*parquetEncoder, *parquet.Schema, error) {
	schema := buildParquetSchema(columns)

	colIndex := make([]int, len(columns))
	for i, col := range columns {
		leaf, ok := schema.Lookup(col.Name)
		if !ok {
			return nil, nil, fmt.Errorf("column %q not found in parquet schema", col.Name)
		}
		colIndex[i] = leaf.ColumnIndex
	}

	return &parquetEncoder{
		columns:  columns,
		colIndex: colIndex,
		incrIdx:  resolveIncrIndex(columns, source),
		rb:       parquet.NewRowBuilder(schema),
		buf:      make([]parquet.Row, 0, parquetFlushRows),
	}, schema, nil
}

// resolveIncrIndex 返回增量字段（目标列名）在列中的下标；无增量字段返回 -1。
func resolveIncrIndex(columns []reader.ColumnMeta, source *config.SourceConfig) int {
	if source == nil || source.IncrField == "" {
		return -1
	}
	incrColumn := source.FieldsMapping.TargetColumn(source.IncrField)
	for i, col := range columns {
		if col.Name == incrColumn {
			return i
		}
	}
	return -1
}

// encodeBatch 将一个 batch 的所有行编码进 pw，达到 flush 阈值时批量写出，并推进水位。
func (e *parquetEncoder) encodeBatch(pw *parquet.GenericWriter[any], batch transform.Batch) error {
	for _, row := range batch.Rows {
		e.rb.Reset()
		// row 与 batch.Columns 同源构造，长度必然一致。
		for i, v := range row {
			val, perr := parquetValue(e.columns[i].Kind, v)
			if perr != nil {
				if !e.warned {
					log.Printf("parquet: failed to convert value for column %q (%v), writing null", e.columns[i].Name, perr)
					e.warned = true
				}
				val = parquet.NullValue()
			}
			e.rb.Add(e.colIndex[i], val)
		}
		e.trackWatermark(row)
		e.buf = append(e.buf, e.rb.Row().Clone())
		if len(e.buf) >= parquetFlushRows {
			if err := e.flush(pw); err != nil {
				return err
			}
		}
	}
	return nil
}

// trackWatermark 依增量列更新已见最大水位。
// 水位以文本形式回填 incr_point 并参与下轮 SQL 的 ${INCR_POINT} 替换，
// 故取规范文本而非 parquet 落地值。
func (e *parquetEncoder) trackWatermark(row []any) {
	if e.incrIdx < 0 || row[e.incrIdx] == nil {
		return
	}
	kind := e.columns[e.incrIdx].Kind
	if v := reader.FormatText(kind, row[e.incrIdx]); wmGreater(kind, v, e.maxWM) {
		e.maxWM = v
	}
}

// flush 将缓冲行写入 pw 并清空缓冲。
func (e *parquetEncoder) flush(pw *parquet.GenericWriter[any]) error {
	if len(e.buf) == 0 {
		return nil
	}
	if _, err := pw.WriteRows(e.buf); err != nil {
		return fmt.Errorf("write parquet rows failed: %w", err)
	}
	e.buf = e.buf[:0]
	return nil
}

// writeObject 从 channel 消费所有 batch，按首个 batch 的列与类型建立 schema，将 parquet 数据
// 流式写入对象存储（经 io.Pipe 边序列化边上传，不落本地临时文件）。
// 若 source 非 nil 且含增量字段，返回其最大值作为水位。
func (d *parquetWriterDialect) writeObject(ctx context.Context, in <-chan transform.Batch, key string, source *config.SourceConfig) (string, error) {
	target := d.store.describe(key)

	firstBatch, foundRows := drainFirstBatch(in)
	if !foundRows {
		log.Printf("parquet write finished: no rows to load (object=%s)", target)
		return "", nil
	}

	enc, schema, err := newParquetEncoder(firstBatch.Columns, source)
	if err != nil {
		return "", err
	}

	// 流式上传：parquet 直接写入 pipe 写端，上传协程从读端消费并 PUT 到对象存储，
	// 避免"先落本地临时文件再上传"的二次写入。
	pr, pipeW := io.Pipe()
	uploadDone := make(chan error, 1)
	go func() {
		uploadDone <- d.store.put(ctx, key, pr)
	}()

	pw := parquet.NewGenericWriter[any](pipeW, schema)

	// abort 用根因错误关闭 pipe 写端，唤醒并等待上传协程结束，再回传该错误。
	abort := func(err error) (string, error) {
		pipeW.CloseWithError(err)
		<-uploadDone
		return "", err
	}

	if err := enc.encodeBatch(pw, firstBatch); err != nil {
		return abort(err)
	}
	for batch := range in {
		if err := enc.encodeBatch(pw, batch); err != nil {
			return abort(err)
		}
	}

	// reader 出错会 cancel(ctx)：此时中止上传，交由上层返回根因错误。
	if err := ctx.Err(); err != nil {
		return abort(err)
	}

	if err := enc.flush(pw); err != nil {
		return abort(err)
	}

	// 刷出 parquet 尾部（footer 等）到 pipe，但不关闭底层 pipe 写端。
	if err := pw.Close(); err != nil {
		return abort(fmt.Errorf("close parquet writer failed: %w", err))
	}

	// 关闭 pipe 写端，向上传协程发出 EOF，触发其读完并完成 PUT。
	if err := pipeW.Close(); err != nil {
		<-uploadDone
		return "", fmt.Errorf("close parquet stream failed: %w", err)
	}

	if err := <-uploadDone; err != nil {
		return "", err
	}

	log.Printf("parquet write finished: object=%s", target)
	return enc.maxWM, nil
}

func (d *parquetWriterDialect) getWatermark(target *config.TargetConfig, source *config.SourceConfig, jobName string) (string, error) {
	if source == nil || source.IncrField == "" {
		return "", nil
	}

	if d.managerConn != nil {
		wm, err := readWatermarkPoint(context.Background(), d.managerConn, target, source, jobName)
		if err != nil {
			return "", err
		}
		if wm != "" {
			return wm, nil
		}
	}

	// 文件目标没有可查询的目标表，只能按字段名推算兜底起点。
	wm := defaultIncrPoint(source.IncrField)
	log.Printf("parquet watermark fallback: using default %s=%s", source.IncrField, wm)
	return wm, nil
}

// buildParquetSchema 依列名与 ColumnKind 构造 parquet schema（全部为 optional 以容纳 NULL）。
func buildParquetSchema(columns []reader.ColumnMeta) *parquet.Schema {
	group := parquet.Group{}
	for _, col := range columns {
		var node parquet.Node
		switch col.Kind {
		case reader.KindInt:
			node = parquet.Int(64)
		case reader.KindFloat:
			node = parquet.Leaf(parquet.DoubleType)
		case reader.KindBool:
			node = parquet.Leaf(parquet.BooleanType)
		case reader.KindTime:
			node = parquet.Timestamp(parquet.Millisecond)
		case reader.KindBytes:
			node = parquet.Leaf(parquet.ByteArrayType)
		default:
			node = parquet.String()
		}
		group[col.Name] = parquet.Optional(node)
	}
	return parquet.NewSchema("etl", group)
}

// parquetValue 将驱动返回的原始值转为对应 ColumnKind 的 parquet 值。
// nil → NULL；类型不符时返回 error，由调用方降级为 NULL 并告警。
//
// 各分支只覆盖 database/sql 扫描进 any 时会产出的类型
// （int64 / float64 / bool / []byte / string / time.Time），
// 不做跨类型的文本解析兜底：驱动的表示差异属于读取端的问题，
// 应由 reader.ValueNormalizer 一次性修正，否则同一份兼容逻辑要在每个 writer 里各写一遍。
func parquetValue(kind reader.ColumnKind, v any) (parquet.Value, error) {
	if v == nil {
		return parquet.NullValue(), nil
	}

	switch kind {
	case reader.KindInt:
		if n, ok := v.(int64); ok {
			return parquet.Int64Value(n), nil
		}
	case reader.KindFloat:
		if f, ok := v.(float64); ok {
			return parquet.DoubleValue(f), nil
		}
	case reader.KindBool:
		switch n := v.(type) {
		case bool:
			return parquet.BooleanValue(n), nil
		case int64:
			// 部分驱动把 BIT / NUMBER(1) 以 0/1 整数返回。
			return parquet.BooleanValue(n != 0), nil
		}
	case reader.KindTime:
		if t, ok := v.(time.Time); ok {
			// 先归一到 UTC 再取毫秒，保留驱动携带的时区信息。
			return parquet.Int64Value(t.UTC().UnixMilli()), nil
		}
	case reader.KindBytes:
		if b, ok := v.([]byte); ok {
			return parquet.ByteArrayValue(b), nil
		}
	default:
		// KindString：文本类与按文本透传的 NUMERIC/DECIMAL 在此统一为 UTF-8 字节。
		return parquet.ByteArrayValue([]byte(reader.FormatText(kind, v))), nil
	}

	return parquet.Value{}, fmt.Errorf("cannot convert %T to %s", v, kind)
}

// wmGreater 判断 a 是否比当前水位 cur 更大（cur 为空视为最小）。
func wmGreater(kind reader.ColumnKind, a, cur string) bool {
	if cur == "" {
		return true
	}
	a, cur = strings.TrimSpace(a), strings.TrimSpace(cur)

	switch kind {
	case reader.KindInt:
		// 整数不借道 float64 比较：雪花 ID 等超过 2^53 的值会丢精度而误判。
		if ai, err := strconv.ParseInt(a, 10, 64); err == nil {
			if ci, err := strconv.ParseInt(cur, 10, 64); err == nil {
				return ai > ci
			}
		}
	case reader.KindFloat:
		if af, err := strconv.ParseFloat(a, 64); err == nil {
			if cf, err := strconv.ParseFloat(cur, 64); err == nil {
				return af > cf
			}
		}
	}

	// 时间戳采用固定布局，字典序与时间序一致；字符串同样按字典序。
	return a > cur
}

// defaultObjectKey 返回 full/initial 模式覆盖写的单对象 key（<table>.parquet）；前缀由 s3Store 追加。
func defaultObjectKey(target *config.TargetConfig) string {
	return sanitizeFileName(target.Table) + ".parquet"
}

// incrementalObjectKey 以本次抽取的起点水位命名增量对象（<table>_<incr_point>.parquet）。
// 起点相同即写同一个对象，失败重跑会覆盖残留而非在桶里堆积孤儿对象，
// 使「上传成功但水位写回失败」的重试幂等。正常调度下水位持续推进，各次增量自然落到不同对象。
// incr_field 是 append/merge 的必填项（见 config.validateSource），故起点必然非空。
func incrementalObjectKey(target *config.TargetConfig, source *config.SourceConfig) string {
	return sanitizeFileName(target.Table) + "_" + sanitizeFileName(source.IncrPoint) + ".parquet"
}

// sanitizeFileName 将表名或水位值转为安全的对象名片段：
// 仅保留字母、数字、'_' 与 '-'，其余字符（'.'、'/'、空格、':' 等）一律替换为 '_'。
func sanitizeFileName(s string) string {
	name := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, strings.TrimSpace(s))
	if name == "" {
		return "data"
	}
	return name
}
