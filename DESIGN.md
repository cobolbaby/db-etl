# Design Document

本文档描述 `db-etl` 的架构设计、核心抽象和各同步模式的实现原理。  
配置字段说明和使用示例请参见 [README.md](./README.md)。

---

## 1. 整体架构

```
                          ┌────────────────────────────────────┐
                          │            main.go                 │
                          │  Worker Pool (min(NumCPU, 4))      │
                          │  Task Channel → runTask()          │
                          └──────────────┬───────────────────  ┘
                                         │ per source
                          ┌──────────────▼───────────────────  ┐
                          │          Pipeline                   │
                          │                                     │
  ┌──────────┐   chan     │  ┌────────────┐   chan   ┌───────┐ │
  │  Reader  │──RowBatch──┼─▶│ Transformer│──Batch───▶ Writer│ │
  └──────────┘            │  └────────────┘          └───────┘ │
                          └────────────────────────────────────┘
                                         ▲                 │
                                         │ Watermark       │ PG COPY
                                         ▼                 │ 或 S3 parquet
                          ┌────────────────────────────────────┐
                          │   manager.job_data_sync            │
                          └────────────────────────────────────┘
```

### 数据流

1. **Reader** 执行 SQL 查询，按 `batch_size` 分批发送 `RowBatch`（`[][]any`）到 channel。
   值保持驱动返回的原始 Go 类型，不做任何面向目标格式的序列化。
2. **Transformer** 多 worker 并发消费 `RowBatch`，附上列元信息并做结构重塑（如 unpivot），输出 `Batch{Columns, Rows}`。
3. **Writer** 消费 `Batch`，按自身目标格式序列化：PG 走 `COPY FROM STDIN`（CSV 文本），S3 走 parquet 编码。

所有阶段通过带缓冲 channel 连接，天然实现**背压（back-pressure）**控制。

### 序列化的归属

值的序列化**不在 reader 或 transform 完成，而是下推到各 writer**。理由是「怎么落地」是目标端的知识：
PG COPY 需要 CSV 文本转义与 `\x` 十六进制字面量，parquet 需要的是物理类型的二进制值——
若在读取端就定死成字符串，parquet 侧只能再解析回来，既损精度也丢时区。

两端通过 `reader.ColumnKind` 这一枚举握手：

```
reader 方言:   源类型名（INT4 / DATETIME2 / NUMBER）→ ColumnKind
               ↓
           ColumnKind: String / Int / Float / Bool / Time / Bytes
               ↓
writer/pgcopy:  ColumnKind + any → COPY CSV 字段文本
writer/parquet: ColumnKind → parquet 物理类型；any → parquet.Value
```

于是 reader 不必知道下游是 PG 还是 S3，writer 也不必认识任何源库的类型名。

---

## 2. 核心抽象

### 2.1 Dialect 模式（Strategy）

Reader 和 Writer 均采用 **Base + Dialect** 结构：

```
BaseReader { conn, Source, dialect readerDialect }
  ├── mssqlDialect   → buildBaseQuery / columnKind / valueNormalizer / quoteIdentifier
  ├── pgDialect      → ...
  └── oracleDialect  → ...

BaseWriter { Target, JobName, dialect writerDialect }
  ├── pgWriterDialect      → writeInitial / writeFull / writeAppend / writeMerge / getWatermark
  └── parquetWriterDialect → 同上，落地到 S3 对象存储
```

- `Base*` 实现通用流程（batch 循环、query context、模式路由）。
- `dialect` 接口封装数据库差异（SQL 语法、标识符引用、类型映射、写入协议等）。
- 新增数据库类型只需实现对应 dialect，无需改动 Base 逻辑。

`readerDialect` 中与类型相关的两个方法职责区分明确：

| 方法 | 职责 |
| --- | --- |
| `columnKind` | 源类型名 → `ColumnKind`，纯映射，决定下游怎么落地 |
| `valueNormalizer` | 修正驱动层的值表示差异，使同一 Kind 在各源库上呈现一致的 Go 类型 |

绝大多数列不需要 normalizer（返回 nil）；目前唯一的使用者是 MSSQL 的
`uniqueidentifier`——go-mssqldb 以 SQL Server 的混合字节序返回 16 字节值，
需按 RFC 4122 重排。修正发生在 `ReadBatch` 内部，writer 拿到的已是规范 UUID 文本。

### 2.2 Factory 模式

`reader.NewReader()` / `writer.NewWriter()` 根据 `DBConfig.Type` 动态构建具体实现，调用方无需关心底层类型。

### 2.3 Worker Pool

`main.go` 使用固定大小 goroutine 池消费 task channel：

- 池大小 = `min(runtime.NumCPU(), 4)`，经验值，避免过多并发连接。
- 每个 worker 独立持有数据库连接，task 之间互不干扰。
- `error_policy` 控制失败行为：`abort` 立即退出 / `continue` 跳过当前 task。

---

## 3. 同步模式实现详解

### 3.1 Copy 模式

最简单的写入路径，适合一次性导入或外部保证幂等的场景。

```
Reader → Transformer → [io.Pipe] → PgConn.CopyFrom(STDIN)
```

**实现技巧：**

| 技巧 | 原理 |
| --- | --- |
| `io.Pipe` 流式传输 | 生产端（CSV 编码）和消费端（COPY 协议）通过 pipe 连接，数据不落盘、不缓存全量 |
| 4MB 写缓冲 | `bytes.Buffer` 初始 4MB，累积到 3MB flush 一次，减少 `write()` 系统调用 |
| COPY goroutine 异步 | COPY 消费在独立 goroutine，生产阻塞即为背压信号 |
| NULL 哨兵 | COPY 的 CSV 格式无法区分「空字符串」与「NULL」，故改用哨兵文本（见下） |

**CSV 编码位置：**`writer/pgcopy.go` 集中了 COPY 格式的全部知识（`encodeCopyValue` / `sanitizeCSV`），
在 COPY 写入时才把 `any` 编码为字段文本；`reader` 与 `transform` 对此一无所知。

| 环节 | 处理 |
| --- | --- |
| NULL | 编码为 `__DB_ETL_NULL__` 哨兵，`COPY ... NULL '__DB_ETL_NULL__'` 据此识别 |
| 空字符串 | 仍编码为空字段，与 NULL 彻底分开（能正确触发 NOT NULL 约束） |
| 二进制（bytea） | 输出 `\x` 十六进制字面量 |
| 控制字符 | 剔除会破坏 CSV 结构的不可见字符（保留 \t \n \r 交由引号包裹） |
| 哨兵撞车 | 源数据恰好等于哨兵文本时强制加引号，确保作为普通文本入库 |

---

### 3.2 Full 模式

全量覆盖，保证目标表数据与源完全一致。

```
BEGIN
  TRUNCATE target_table        -- 尝试，带 lock_timeout
  ↓ 超时？
  DELETE FROM target_table     -- 退避
  COPY INTO target_table       -- 在同一事务连接上执行
COMMIT
```

**实现技巧：**

| 技巧 | 原理 |
| --- | --- |
| TRUNCATE + COPY 同事务 | 原子性保证：失败自动回滚，不会出现"清空了但没写入"的中间态 |
| Lock Timeout 退避 | `SET LOCAL lock_timeout = '30s'`；TRUNCATE 需要 ACCESS EXCLUSIVE 锁，若被阻塞则超时后退化为 `DELETE FROM`（仅需 ROW EXCLUSIVE 锁），避免长时间阻塞其他会话 |
| 延迟启动 `drainFirstBatch()` | 先从 channel 取第一个非空 batch，若 Reader 无数据直接跳过，不执行无意义的 TRUNCATE |
| `CREATE TEMP TABLE ... ON COMMIT DROP` | Full 模式不需要 staging 表，但 Merge/Append 需要——临时表随事务结束自动清理 |

---

### 3.3 Append 模式

追加写入 + Watermark 更新，适合只增不改的流水表。

```
BEGIN
  CREATE TEMP TABLE staging (LIKE target) ON COMMIT DROP
  COPY INTO staging
  INSERT INTO target SELECT * FROM staging
  UPDATE watermark = MAX(incr_field) FROM staging
COMMIT
```

**实现技巧：**

| 技巧 | 原理 |
| --- | --- |
| Staging 中转 | 先 COPY 到 temp table 再 `INSERT INTO ... SELECT`，比逐行 INSERT 快 1~2 个数量级 |
| Watermark 原子更新 | 水位更新与数据写入在同一事务，保证一致性——不会出现"数据写了但水位没更新"导致重复同步 |
| 分段提交 `commit_batch_size` | 超大表场景，每 N 个 batch 提交一次事务并推进水位；中断后从上次水位续传，避免从头同步 |
| Watermark Fallback 链 | ① 查 `job_data_sync.incr_point` → ② 查目标表 `MAX(incr_field)` → ③ 按字段名推断默认值 |

---

### 3.4 Merge 模式

增量 Upsert，适合源端有更新的业务表。

```
BEGIN
  CREATE TEMP TABLE staging (LIKE target) ON COMMIT DROP
  COPY INTO staging
  DELETE FROM target t USING staging s WHERE t.pk = s.pk
  INSERT INTO target SELECT * FROM staging
  UPDATE watermark
COMMIT
```

**实现技巧：**

| 技巧 | 原理 |
| --- | --- |
| DELETE + INSERT 代替 ON CONFLICT | 不依赖 UNIQUE 约束，支持任意复合 PK（逗号分隔），兼容 Greenplum 等不完整支持 `ON CONFLICT` 的引擎 |
| JOIN 条件自动构建 | `buildJoinCondition("t", "s", "pk1,pk2")` → `t.pk1=s.pk1 AND t.pk2=s.pk2` |
| 与 Append 共用核心逻辑 | `writeIncrOnce` / `writeIncrSegmented` 通过 `needDelete bool` 参数区分，最大化代码复用 |
| 分段模式防死锁 | `select { case feedCh <- batch; case <-copyDone }` 同时监听两个 channel，COPY goroutine 异常退出时主循环不会永久阻塞 |

---

### 3.5 分段提交（Segmented Commit）

当 `commit_batch_size > 0` 时，Append/Merge 切换为分段模式：

```
for each segment (N batches):
    BEGIN
      CREATE TEMP staging
      COPY N batches → staging
      [DELETE target WHERE pk IN staging]   -- merge only
      INSERT INTO target FROM staging
      UPDATE watermark = MAX(incr_field) of this segment
    COMMIT
```

**关键约束：**

- 数据必须按 `incr_field` 有序（框架自动补 `ORDER BY`），保证每段的水位单调递增。
- 每段独立事务，中断后重启自动从已提交的最新水位继续。

---

### 3.6 S3 / Parquet 落地

当 `target.s3` 引用了 `s3[]` 中的存储时，写入端切换为 `parquetWriterDialect`。

```
buildParquetSchema(columns)   → 按 ColumnKind 生成 parquet schema（全部 Optional）
encodeBatch                   → RowBuilder 逐行构造 parquet.Value
flush (每 1024 行)            → GenericWriter[any]
writeObject                   → io.Pipe → minio PutObject（size = -1，流式上传）
```

| ColumnKind | Parquet 物理类型 |
| --- | --- |
| `KindInt` | `INT64` |
| `KindFloat` | `DOUBLE` |
| `KindBool` | `BOOLEAN` |
| `KindTime` | `INT64` + `Timestamp(Millisecond)`，值按 `t.UTC().UnixMilli()` |
| `KindBytes` | `BYTE_ARRAY` |
| 其余 | `BYTE_ARRAY` + `String()` 逻辑类型 |

**与 PG 落地的关键差异：**

| 维度 | PG COPY | S3 Parquet |
| --- | --- | --- |
| 值形态 | 一律 CSV 文本 | 保留原生类型（时间戳/浮点/二进制不经文本往返） |
| 事务性 | 水位与数据在同一事务内提交 | 对象存储无事务，水位单独提交 |
| 幂等保障 | 事务回滚 | 对象 key 确定：`<table>.parquet` / `<table>_<incr_point>.parquet`，重跑覆盖同名对象 |

由于对象存储没有事务，增量模式的对象 key 刻意包含水位值：中断重跑会写到同一个 key 上并整体覆盖，
从而在没有事务的前提下获得幂等性。

另外，对象存储无法按 pk 原地删除/更新，`merge` 退化为与 `append` 相同的追加语义，去重交给下游查询。

---

## 4. Reader 端设计

### 4.1 查询构建

```
resolveProjection()          → fields_mapping 或 *
buildWhereClause()           → where_statement AND 1=1
buildBaseQuery(emptyResult)  → dialect 组装完整 SQL
buildReadQuery()             → 追加增量条件 + ORDER BY
```

- **占位符模式**：SQL 中包含 `${SRC_INCR_FIELD}` / `${INCR_POINT}` 时，框架仅做字符串替换，不追加额外条件或排序——适合复杂自定义 SQL。
- **自动模式**：否则框架自动追加 `AND incr_field > 'value'` + `ORDER BY`。

### 4.2 类型处理

`GetColumnMeta()` 只做一次列类型探测（`WHERE 1=0`），根据 `sql.ColumnType.DatabaseTypeName()`
为每一列返回 `ColumnMeta{Name, TypeName, Kind}`。`Kind` 由各方言的 `columnKind` 映射：

| ColumnKind | 覆盖类型 | 驱动返回的 Go 类型 |
| --- | --- | --- |
| `KindString` | 文本类、NUMERIC/DECIMAL/NUMBER、PG 数组字面量 | `string` / `[]byte` |
| `KindInt` | INT2/INT4/INT8、TINYINT..BIGINT | `int64` |
| `KindFloat` | FLOAT4/FLOAT8、REAL/FLOAT | `float64` |
| `KindBool` | BOOL、MSSQL BIT | `bool` 或 0/1 整数 |
| `KindTime` | DATE/TIME/TIMESTAMP/DATETIME2/DATETIMEOFFSET | `time.Time` |
| `KindBytes` | BYTEA、VARBINARY、Oracle RAW/BLOB | `[]byte` |

**为什么没有 `KindDecimal`：**NUMERIC / DECIMAL / NUMBER 一律归入 `KindString`。
三个驱动都以文本返回这类列，按字符串透传恰好保住完整精度；转成 `float64` 反而会丢精度。

**文本归一：**`reader.FormatText(kind, v)` 是「Go 值 → 规范文本」的唯一实现，
不含任何目标格式的转义或 NULL 表示（那些由各 writer 在其之上叠加）。
时间采用 `TimestampLayout = "2006-01-02 15:04:05.999999999"`：保留至纳秒且自动去除尾零，
可覆盖 Oracle `TIMESTAMP(9)` 与 MSSQL `DATETIME2(7)`，且是三个源库都能直接解析的字面量格式。

### 4.3 Batch Channel 缓冲

`make(chan RowBatch, 8)` — 8 个 batch 的缓冲平衡 Reader（网络 IO 密集）和 Writer（磁盘 IO 密集）的速度差异。

---

## 5. Transform 层设计

Transform 负责**附列元信息与结构重塑**，不做值的序列化：

```go
type Batch struct {
    Columns []reader.ColumnMeta
    Rows    [][]any
}

type Transformer interface {
    Transform(batch reader.RowBatch) Batch   // 入口：原始行 → 带列信息的批
}

type Step interface {
    Transform(batch Batch) Batch             // 可链式叠加的转换步骤
}
```

- `baseTransformer` 只把 `GetColumnMeta()` 的结果附到 batch 上；无 transform 配置时即为全部逻辑。
- 配置了 transform 时，`chainTransformer` 在其后依次执行各 `Step`。
- Pipeline 内使用 `min(NumCPU, 2)` 个 worker 并发执行 Transform，输出到 `batchChan`（缓冲 4）。

### 5.1 Unpivot（列转行）

把宽表的一组列（如 `Day1..Day31`）展开成 `key + value` 两列多行，其余列作为标识列逐行重复。
展开在 **ETL 侧**而非源端 SQL：若在源端用 `UNION ALL` 展开，标识列会重复传输 N 遍，
源库→ETL 的网络量成倍放大。

一个必要的细节：`value_field` 汇聚的是**多个异构源列**的取值，无单一源类型，
固定为 `KindString`。因此每个值在汇入前先按**其源列的 Kind** 调 `reader.FormatText` 渲染为规范文本；
否则 `time.Time` 会落到 `fmt.Sprint` 的默认格式（`2024-01-01 00:00:00 +0800 CST`）。
`nil` 保持为 `nil`，令 NULL 语义完整传递到 writer。

---

## 6. Watermark 机制

### 6.1 存储

水位存储在目标库的 `manager.job_data_sync` 表中，按 (job_name + source identity + target identity) 唯一定位。

### 6.2 读取（启动时）

```
getWatermark()
  → 查 job_data_sync.incr_point
  → 若空：SELECT MAX(incr_field) FROM target_table
  → 若仍空：defaultIncrPoint(field_name)
       含 time/date/cdt/udt → "1970-01-01 00:00:00.000"
       其他                  → "1"
```

### 6.3 写入（提交时）

采用 **Upsert 模式**：先 UPDATE（按匹配键），受影响行数为 0 则 INSERT。保证首次运行和后续运行均可正确写入。

不用 `INSERT ... ON CONFLICT` 是有意为之：一来水位表不一定长期留在 PostgreSQL 上，
二来 Greenplum 对 `ON CONFLICT` 支持不完整，UPDATE→INSERT 是可移植的写法。

> 代价：该写法在并发下可能插入重复行（READ COMMITTED 下两个事务都看不到对方未提交的行，
> 包事务也不能解决，只有唯一索引能仲裁）。当前靠调度侧保证同一任务不并发执行。

---

## 7. 错误处理策略

| 层级 | 策略 |
| --- | --- |
| Task 级 | `error_policy: abort` 立即退出 / `continue` 跳过当前 task |
| Source 级 | 先按 `retry` 策略指数退避重试（仅可重试错误），仍失败则继续下一个 source |
| 错误分类 | `util/pgerr.go` 等将 SQLSTATE 22/23/42 类（数据/约束/语法）标为不可重试 |
| Pipeline 内部 | Reader 出错时 cancel ctx 令 writer 事务回滚；返回时 reader 错误优先 |
| 分段模式 | 每段独立事务，已提交段不回滚；失败段回滚，程序退出后可从水位续传 |

---

## 8. 并发模型总结

```
main goroutine
 ├── Worker Pool (min(NumCPU, 4)) ── 消费 taskCh
 │    └── per source:
 │         ├── Reader goroutine      ── DB query → rowChan (buf=8)
 │         ├── Transform workers (min(NumCPU, 2)) ── rowChan → batchChan (buf=4)
 │         └── Writer (当前 goroutine) ── batchChan → COPY / parquet
 │              └── upload goroutine  ── pipe consumer（COPY 协议 或 S3 PutObject）
 └── wg.Wait()
```

每个 source 的管道占 `1 reader + 2 transformers + 1 pipe consumer` 个 goroutine（writer 本体复用 worker goroutine），
并发度上限为 `最大 4 个 task × 4 = 16` 个（不含 runtime 内部）。

错误优先级：reader 出错时会 `cancel(ctx)`，令 writer 的事务以 `context.Canceled` 中止并回滚，
避免 full/initial 模式下提交被截断的部分数据；`RunPipeline` 返回时 reader 错误优先于 writer 错误，
因为前者是根因。

---

## 9. 扩展点

| 方向 | 当前状态 | 扩展方式 |
| --- | --- | --- |
| 新数据源 | MSSQL / PG / GP / Oracle | 实现 `readerDialect` 接口 |
| 新目标端 | PG / GP（COPY）、S3（parquet） | 实现 `writerDialect` 接口 |
| 新列类型 | 6 种 `ColumnKind` | 新增 Kind + 各 writer 补全分支 |
| 自定义 Transform | unpivot | 实现 `transform.Step` 接口 |
| 监控指标 | log 打印耗时 | 注入 metrics collector |
| 重试机制 | `util/retry.go` 指数退避 | 调整 `retry` 配置项 |
| 密码管理 | YAML 支持 `${ENV}` 引用 | 对接 Vault 等外部密钥源 |
