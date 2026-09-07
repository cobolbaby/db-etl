package writer

import (
	"context"
	"db-etl/config"
	"db-etl/reader"
	"db-etl/util"
	"fmt"
	"io"
	"log"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"
	"github.com/parquet-go/parquet-go"
)

// parquetWriterDialect 将 Batch 序列化为 parquet 并流式上传到对象存储。
//   - full/initial：覆盖写单个 <table>.parquet 对象（每次全量刷新，不分卷）。
//   - append/merge：每次增量写一个以起点水位命名的对象，并把水位写回 manager.job_data_sync；
//     当 commit_batch_size>0 且可写回水位时改为分段断点续传，每块独立成对象并逐块提交水位
//     （见 writeChunkedObjects）。
//
// metaConn 指向存放 manager.job_data_sync 的 PostgreSQL；为 nil 时跳过水位写回/读取
// （退化为无状态增量，起点由 defaultIncrPoint 兜底）。
// 与 PG writer 不同：对象存储上传没有事务可挂靠，水位只能走这条独立连接写，
// 因此存在「上传成功但水位写失败」的窗口。
type parquetWriterDialect struct {
	store    *s3Store
	metaConn *pgx.Conn
	target   *config.TargetConfig
	jobName  string
}

// NewParquetWriter 构建写 parquet 到对象存储的 Writer。
// s3 提供对象存储连接信息；metaDB 提供水位写回连接，为零值（未配置 meta_db）时不建连接。
func NewParquetWriter(s3 config.S3Config, metaDB config.DBConfig, target *config.TargetConfig, jobName string) (Writer, error) {
	if target == nil {
		return nil, util.NonRetryable(fmt.Errorf("target config is required for parquet writer"))
	}

	store, err := newS3Store(s3)
	if err != nil {
		return nil, err
	}

	// Database 是每个 datasource 的必填项（见 config.validateDatabases），
	// 为空即表示未配置 meta_db，此时不建连接、跳过水位读写。
	var metaConn *pgx.Conn
	if metaDB.Database != "" {
		conn, err := pgx.Connect(context.Background(), metaDB.DSN())
		if err != nil {
			return nil, fmt.Errorf("meta DB connect failed: %w", err)
		}
		metaConn = conn
	}

	base := &BaseWriter{
		Target: target,
	}
	base.dialect = &parquetWriterDialect{store: store, metaConn: metaConn, target: target, jobName: jobName}

	return base, nil
}

// close 关闭 manager 连接（若有）。
func (d *parquetWriterDialect) close(ctx context.Context) error {
	if d.metaConn == nil {
		return nil
	}
	return d.metaConn.Close(ctx)
}

func (d *parquetWriterDialect) writeInitial(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error {
	target := d.target
	jobName := d.jobName
	// initial 视为一次性全量：覆盖写单对象；成功后置 inuse=false，避免下次重复回填。
	if _, err := d.writeSingleObject(ctx, in, defaultObjectKey(target), source); err != nil {
		return err
	}
	if d.metaConn != nil {
		if _, err := execDeactivateInitialJob(ctx, d.metaConn, target, source, jobName); err != nil {
			return err
		}
		log.Printf("initial parquet task done, set inuse=false (table=%s)", target.Table)
	}
	return nil
}

func (d *parquetWriterDialect) writeFull(ctx context.Context, in <-chan reader.Batch) error {
	target := d.target
	// full 全量刷新：覆盖写单对象。无水位、无 source 依赖。
	_, err := d.writeSingleObject(ctx, in, defaultObjectKey(target), nil)
	return err
}

func (d *parquetWriterDialect) writeAppend(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error {
	return d.writeIncremental(ctx, in, source)
}

func (d *parquetWriterDialect) writeMerge(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error {
	target := d.target
	// 对象存储不可按 PK 原地删除/更新，merge 与 append 一致：追加新对象，去重交由下游查询处理。
	log.Printf("parquet target does not support in-place merge by pk; writing incremental object for table=%s (downstream must dedupe by pk=%s)", target.Table, target.PK)
	return d.writeIncremental(ctx, in, source)
}

// writeIncremental 每次增量写一个新对象，并把最大水位写回 manager。
//
// 当 target.CommitBatchSize > 0 且可写回水位时切换为分段断点续传（见 writeChunkedObjects）：
// 每满 CommitBatchSize 个 batch 收尾一个对象并提交其水位，中断后重跑从上次已提交的断点续传，
// 而非从任务起点全量重导。否则退化为单对象：整段写完后一次性提交水位。
func (d *parquetWriterDialect) writeIncremental(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error {
	target := d.target

	// 分段断点续传要求：能写回水位（metaConn 非空）且有增量字段可作断点。
	// 三者缺一则无法按块提交/续传，退化为单对象一次性提交。
	if target.CommitBatchSize > 0 && d.metaConn != nil && source != nil && source.IncrField != "" {
		return d.writeChunkedObjects(ctx, in, source)
	}

	key := incrementalObjectKey(target, source)
	wm, err := d.writeSingleObject(ctx, in, key, source)
	if err != nil {
		return err
	}

	// 无数据或未追踪到水位则不推进（保持既有 incr_point 不变）。
	if wm == "" || d.metaConn == nil || source == nil {
		return nil
	}
	return upsertWatermark(ctx, d.metaConn, wm, target, source, d.jobName)
}

// writeChunkedObjects 把增量写入拆成若干块做断点续传，语义与 PG 的 writeIncrChunked 对齐：
// 每满 CommitBatchSize 个 batch 收尾一个独立 parquet 对象，提交其最大水位，并把 source.IncrPoint
// 推进到该值。若后续块失败并触发外层 util.Retry 重跑整条 pipeline，reader 会从已提交的断点续读，
// 已成功上传的块不再重复导出。
//
// 每块以「当前 source.IncrPoint」（即上一块的水位）命名，故对象名随水位单调推进、天然唯一，
// 无需 _partNNNN 之类的序号后缀；重跑只覆盖尚未提交的那一块，已提交的对象名互不相同、不会被误覆盖。
// 这依赖 reader 在 commit_batch_size>0 时强制的 ORDER BY incr_field ASC（保证每块水位单调递增）。
func (d *parquetWriterDialect) writeChunkedObjects(ctx context.Context, in <-chan reader.Batch, source *config.SourceConfig) error {
	target := d.target

	firstBatch, foundRows := drainFirstBatch(in)
	if !foundRows {
		log.Printf("parquet incremental finished: no rows to load (table=%s)", target.Table)
		return nil
	}

	enc, err := newParquetEncoder(target.Table, firstBatch.Columns, source)
	if err != nil {
		return err
	}

	chunkBatches := target.CommitBatchSize

	var (
		part         *parquetPart
		batchInChunk int
	)

	// openChunk 按当前 IncrPoint 命名并打开下一个块对象。
	openChunk := func() {
		part = d.startPart(ctx, incrementalObjectKey(target, source), enc.schema)
		batchInChunk = 0
	}

	// commitChunk 收尾对象、提交本块水位并推进断点。
	commitChunk := func() error {
		if err := part.finish(); err != nil {
			return err
		}
		// 行有序（ORDER BY ASC），此刻 enc 的全局最大值即本块最大水位。
		wm := enc.watermark()
		if wm != "" {
			if err := upsertWatermark(ctx, d.metaConn, wm, target, source, d.jobName); err != nil {
				return err
			}
			// 推进断点：既供下一块对象命名，也让失败重跑从此处续传而非从任务起点重导。
			source.IncrPoint = wm
		}
		rgCount, rgRows := part.rowGroups()
		log.Printf("parquet chunk finished: object=%s watermark=%s row_groups=%d rows=%v", d.store.describe(part.key), wm, rgCount, rgRows)
		part = nil
		return nil
	}

	// writeOne 把一个 batch 写进当前块：必要时先开块，写满 chunkBatches 后提交收块。
	writeOne := func(batch reader.Batch) error {
		if part == nil {
			openChunk()
		}
		if err := enc.encodeBatch(part.pw, batch); err != nil {
			return part.abort(err)
		}
		batchInChunk++
		if batchInChunk >= chunkBatches {
			return commitChunk()
		}
		return nil
	}

	if err := writeOne(firstBatch); err != nil {
		return err
	}
	for batch := range in {
		if err := writeOne(batch); err != nil {
			return err
		}
	}

	// reader 出错会 cancel(ctx)：必须在收尾提交前拦截，中止仍打开的块，
	// 否则会把不完整的末块 finish、上传并提交其水位。交由上层返回根因错误。
	if err := ctx.Err(); err != nil {
		if part != nil {
			return part.abort(err)
		}
		return err
	}

	// 收尾最后一个未满的块。
	if part != nil {
		if err := commitChunk(); err != nil {
			return err
		}
	}
	return nil
}

// writeSingleObject 从 channel 消费所有 batch，按首个 batch 的列与类型建立 schema，将 parquet 数据
// 流式写入对象存储的单个对象（经 io.Pipe 边序列化边上传，不落本地临时文件）。
// parquet schema 根节点名复用目标表名。
//
// full/initial 全量与「不满足断点续传条件」的增量都走这里：整段写完后一次性收尾。
// 增量的分段断点续传见 writeChunkedObjects。
//
// 若 source 非 nil 且含增量字段，返回其最大值作为水位。
func (d *parquetWriterDialect) writeSingleObject(ctx context.Context, in <-chan reader.Batch, key string, source *config.SourceConfig) (string, error) {
	firstBatch, foundRows := drainFirstBatch(in)
	if !foundRows {
		log.Printf("parquet write finished: no rows to load (object=%s)", d.store.describe(key))
		return "", nil
	}

	enc, err := newParquetEncoder(d.target.Table, firstBatch.Columns, source)
	if err != nil {
		return "", err
	}

	part := d.startPart(ctx, key, enc.schema)

	if err := enc.encodeBatch(part.pw, firstBatch); err != nil {
		return "", part.abort(err)
	}
	for batch := range in {
		if err := enc.encodeBatch(part.pw, batch); err != nil {
			return "", part.abort(err)
		}
	}

	// reader 出错会 cancel(ctx)：必须在收尾前拦截，中止对象写入，
	// 否则会把不完整的对象 finish 并上传上去。交由上层返回根因错误。
	if err := ctx.Err(); err != nil {
		return "", part.abort(err)
	}

	if err := part.finish(); err != nil {
		return "", err
	}
	rgCount, rgRows := part.rowGroups()
	log.Printf("parquet write finished: object=%s row_groups=%d rows=%v", d.store.describe(key), rgCount, rgRows)

	return enc.watermark(), nil
}

func (d *parquetWriterDialect) getWatermark(source *config.SourceConfig) (string, error) {
	target := d.target
	jobName := d.jobName
	if source == nil || source.IncrField == "" {
		return "", nil
	}

	if d.metaConn != nil {
		wm, err := readWatermarkPoint(context.Background(), d.metaConn, target, source, jobName)
		if err != nil {
			return "", err
		}
		if wm != "" {
			return wm, nil
		}
	}

	// 文件目标没有可查询的目标表，只能按字段名推算兑底起点。
	wm := defaultIncrPoint(source.IncrField)
	log.Printf("parquet watermark fallback: using default %s=%s", source.IncrField, wm)
	return wm, nil
}

// parquetEncoder 持有单个对象的编码状态：schema、列映射、类型、增量水位追踪与行缓冲，
// 负责把 Batch 逐行编码写入 parquet.GenericWriter。
type parquetEncoder struct {
	schema   *parquet.Schema
	columns  []reader.ColumnMeta
	colIndex []int             // 输入列序 → schema 叶子列序（parquet.Group 为 map，叶子顺序由库决定）
	incrIdx  int               // 增量字段所在列下标，-1 表示无
	incrKind reader.ColumnKind // 增量列的语义类别，缓存以免逐行查
	rb       *parquet.RowBuilder
	buf      []parquet.Row
	hasMax   bool // 是否已见过非 NULL 的增量值
	maxVal   any  // 已见最大增量值（原生类型，仅在结束时格式化为文本水位）
	warned   bool // 是否已打印过转换失败告警（避免刷屏）
}

// newParquetEncoder 依首个 batch 的列信息构造 schema 与编码器。schemaName 作为 parquet schema 根节点名（复用目标表名）。
func newParquetEncoder(schemaName string, columns []reader.ColumnMeta, source *config.SourceConfig) (*parquetEncoder, error) {
	schema := buildParquetSchema(schemaName, columns)

	// buildParquetSchema 以列名为 map 键，重名列会互相覆盖；字段数对不上即说明存在重名。
	// 若放任不管，两个输入列会映射到同一个叶子列而写出错乱的行。
	if len(schema.Fields()) != len(columns) {
		return nil, util.NonRetryable(fmt.Errorf(
			"duplicate column names in result set: %d columns map to only %d parquet fields (columns: %v)",
			len(columns), len(schema.Fields()), reader.Batch{Columns: columns}.ColumnNames()))
	}

	// schema 由 columns 构造且已确认无重名，故每个列名必然能查到对应叶子。
	colIndex := make([]int, len(columns))
	for i, col := range columns {
		leaf, _ := schema.Lookup(col.Name)
		colIndex[i] = leaf.ColumnIndex
	}

	incrIdx := resolveIncrIndex(columns, source)
	var incrKind reader.ColumnKind
	if incrIdx >= 0 {
		incrKind = columns[incrIdx].Kind
	}

	return &parquetEncoder{
		schema:   schema,
		columns:  columns,
		colIndex: colIndex,
		incrIdx:  incrIdx,
		incrKind: incrKind,
		rb:       parquet.NewRowBuilder(schema),
	}, nil
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

// encodeBatch 将一个 batch 的所有行编码后一次性写入 pw，并推进水位。
// 一次 WriteRows 写完整个 batch：parquet 内部自行按 row group 缓冲/落盘（见 MaxRowsPerRowGroup），
// 无需在此再叠一层按行数的中间批次；e.buf 仅作跨 batch 复用的临时暂存。
func (e *parquetEncoder) encodeBatch(pw *parquet.GenericWriter[any], batch reader.Batch) error {
	e.buf = e.buf[:0]
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
	}
	if len(e.buf) == 0 {
		return nil
	}
	if _, err := pw.WriteRows(e.buf); err != nil {
		return fmt.Errorf("write parquet rows failed: %w", err)
	}
	return nil
}

// trackWatermark 依增量列更新已见最大水位。
// 逐行只做原生类型比较（int64/float64/time.Time），不在热路径上格式化/解析字符串；
// 真正的文本水位由 watermark() 在结束时一次性生成。
func (e *parquetEncoder) trackWatermark(row []any) {
	if e.incrIdx < 0 {
		return
	}
	v := row[e.incrIdx]
	if v == nil {
		return
	}
	if !e.hasMax || rawWatermarkGreater(e.incrKind, v, e.maxVal) {
		e.hasMax = true
		e.maxVal = v
	}
}

// watermark 把已见最大增量值格式化为规范文本水位（回填 incr_point 并参与下轮 ${INCR_POINT} 替换）。
// 未见任何非 NULL 增量值时返回空串。
func (e *parquetEncoder) watermark() string {
	if !e.hasMax {
		return ""
	}
	return reader.FormatText(e.incrKind, e.maxVal)
}

// parquetPart 封装单个 parquet 对象的写入生命周期：io.Pipe + 上传协程 + GenericWriter。
// dialect 往 pw 写行，上传协程从 pipe 读端流式 PUT 到对象存储，避免"先落本地临时文件再上传"的二次写入。
type parquetPart struct {
	key        string
	pw         *parquet.GenericWriter[any]
	pipeW      *io.PipeWriter
	uploadDone chan error
}

// startPart 打开一个新 parquet 对象：建立 pipe、拉起上传协程、构造 GenericWriter。
func (d *parquetWriterDialect) startPart(ctx context.Context, key string, schema *parquet.Schema) *parquetPart {
	pr, pipeW := io.Pipe()
	uploadDone := make(chan error, 1)
	go func() {
		uploadDone <- d.store.put(ctx, key, pr)
	}()

	opts := []parquet.WriterOption{
		schema,
		// 按列 Zstd 压缩：parquet 列内同类型数据高度冗余（尤其 JSON 文本列跨行重复大量 key），
		// 压缩率远高于不压；Zstd 在压缩率/CPU 上优于 Snappy/Gzip，作为离线 ETL 默认编码。
		parquet.Compression(&parquet.Zstd),
		// 限制单个 row group 的行数：不设时整个对象为单个 row group，须等 Close 才落盘，
		// 峰值内存随对象总行数增长；设为正值可切分多个 row group，压低峰值内存并利于下游并行读。
		parquet.MaxRowsPerRowGroup(d.target.MaxRowsPerRowGroup),
		// 写入方标识与血缘元数据：落进 footer，供下游/运维直接读出对象由谁、哪个 job、哪张源表产出，
		// 便于溯源排查（不影响数据本身，读取端可忽略）。
		parquet.CreatedBy("db-etl", "", ""),
		parquet.KeyValueMetadata("db-etl.job", d.jobName),
		parquet.KeyValueMetadata("db-etl.table", d.target.Table),
	}
	pw := parquet.NewGenericWriter[any](pipeW, opts...)
	return &parquetPart{key: key, pw: pw, pipeW: pipeW, uploadDone: uploadDone}
}

// abort 用根因错误关闭 pipe 写端，唤醒并等待上传协程结束，再回传该错误。
func (p *parquetPart) abort(err error) error {
	p.pipeW.CloseWithError(err)
	<-p.uploadDone
	return err
}

// rowGroups 返回本对象已写入的 row group 数量及各组行数；须在 finish（即 pw.Close 写完 footer）
// 之后调用，否则 File() 尚不可用返回 0/nil。用于日志核对 MaxRowsPerRowGroup 的切分是否生效。
func (p *parquetPart) rowGroups() (int, []int64) {
	fv := p.pw.File()
	if fv == nil {
		return 0, nil
	}
	rgs := fv.Metadata().RowGroups
	rows := make([]int64, len(rgs))
	for i := range rgs {
		rows[i] = rgs[i].NumRows
	}
	return len(rgs), rows
}

// finish 收尾当前对象：pw 关闭触发写入 footer，关闭 pipe 触发上传读完并完成 PUT，返回上传结果。
func (p *parquetPart) finish() error {
	if err := p.pw.Close(); err != nil {
		return p.abort(fmt.Errorf("close parquet writer failed: %w", err))
	}
	// 关闭 pipe 写端，向上传协程发出 EOF，触发其读完并完成 PUT。
	if err := p.pipeW.Close(); err != nil {
		<-p.uploadDone
		return fmt.Errorf("close parquet stream failed: %w", err)
	}
	return <-p.uploadDone
}

// buildParquetSchema 依列名与 ColumnKind 构造 parquet schema（全部为 optional 以容纳 NULL）。
// name 为 schema 根节点名，复用目标表名（已由 config.validateTarget 保证非空）。
func buildParquetSchema(name string, columns []reader.ColumnMeta) *parquet.Schema {
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
			node = parquet.Timestamp(parquet.Microsecond)
		case reader.KindBytes:
			node = parquet.Leaf(parquet.ByteArrayType)
		default:
			node = parquet.String()
		}
		group[col.Name] = parquet.Optional(node)
	}
	return parquet.NewSchema(name, group)
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
			// 归一到 UTC 再取微秒。无时区列已由 reader.NaiveTimeNormalizer 贴上源库时区，
			// 带时区列驱动本就返回正确瞬时，故此处 UTC 归一得到的是正确的绝对时刻。
			return parquet.Int64Value(t.UTC().UnixMicro()), nil
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

// rawWatermarkGreater 判断原生值 a 是否比当前最大值 cur 更大。
// int64/float64/time.Time 直接原生比较；其余类别（文本等）回退到规范文本的字典序比较。
// 调用方已保证 cur 非 nil（hasMax），故无需处理空值。
func rawWatermarkGreater(kind reader.ColumnKind, a, cur any) bool {
	switch kind {
	case reader.KindInt:
		av, aok := a.(int64)
		cv, cok := cur.(int64)
		if aok && cok {
			return av > cv
		}
	case reader.KindFloat:
		av, aok := a.(float64)
		cv, cok := cur.(float64)
		if aok && cok {
			return av > cv
		}
	case reader.KindTime:
		av, aok := a.(time.Time)
		cv, cok := cur.(time.Time)
		if aok && cok {
			return av.After(cv)
		}
	}
	// 类型不符或文本类：回退到文本字典序（时间戳固定布局，字典序与时间序一致）。
	return reader.FormatText(kind, a) > reader.FormatText(kind, cur)
}

// defaultObjectKey 返回 full/initial 模式覆盖写的单对象 key（<table>.parquet）；前缀由 s3Store 追加。
// 表名已在配置加载阶段校验（见 config.ValidateTableName），仅含字母、数字、'_'、'.'，可直接拼接。
func defaultObjectKey(target *config.TargetConfig) string {
	return target.Table + ".parquet"
}

// incrementalObjectKey 以本次抽取的起点水位命名增量对象（<table>_<incr_point>.parquet）。
// 起点相同即写同一个对象，失败重跑会覆盖残留而非在桶里堆积孤儿对象，
// 使「上传成功但水位写回失败」的重试幂等。正常调度下水位持续推进，各次增量自然落到不同对象。
// s3 目标的 append/merge 必须配置 incr_field（见 config.validateSource），故起点必然非空。
func incrementalObjectKey(target *config.TargetConfig, source *config.SourceConfig) string {
	return target.Table + "_" + sanitizeWatermark(source.IncrPoint) + ".parquet"
}

// sanitizeWatermark 将水位值（整型或时间戳）转为安全的对象名片段：
// 仅保留字母与数字，其余字符（'-'、空格、':'、'.' 等）一律删除。
// 比如 2026-09-07 12:34:56.789 归一为紧凑的 20260907123456789。
func sanitizeWatermark(s string) string {
	name := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r):
			return r
		default:
			return -1
		}
	}, strings.TrimSpace(s))
	if name == "" {
		return "0"
	}
	return name
}
