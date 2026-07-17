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
  │  Reader  │──RowBatch──┼─▶│ Transformer│──CSVBat──▶ Writer│ │
  └──────────┘            │  └────────────┘          └───────┘ │
                          └────────────────────────────────────┘
                                         ▲
                                         │ Watermark
                                         ▼
                          ┌────────────────────────────────────┐
                          │   manager.job_data_sync         │
                          └────────────────────────────────────┘
```

### 数据流

1. **Reader** 执行 SQL 查询，按 `batch_size` 分批发送 `RowBatch`（`[][]any`）到 channel。
2. **Transformer** 多 worker 并发消费 `RowBatch`，按列类型将 `any` 序列化为 CSV `string`，输出 `CSVBatch`。
3. **Writer** 消费 `CSVBatch`，通过 PostgreSQL `COPY FROM STDIN` 协议高速写入目标库。

所有阶段通过带缓冲 channel 连接，天然实现**背压（back-pressure）**控制。

---

## 2. 核心抽象

### 2.1 Dialect 模式（Strategy）

Reader 和 Writer 均采用 **Base + Dialect** 结构：

```
BaseReader { DB, Source, dialect readerDialect }
  ├── MSSQLDialect   → buildBaseQuery / getColumnHandler / quoteIdentifier
  └── PGDialect      → ...

BaseWriter { Target, JobName, dialect writerDialect }
  └── pgWriterDialect → writeCopy / writeFull / writeAppend / writeMerge / getWatermark
```

- `Base*` 实现通用流程（batch 循环、query context、模式路由）。
- `dialect` 接口封装数据库差异（SQL 语法、标识符引用、COPY 协议等）。
- 新增数据库类型只需实现对应 dialect，无需改动 Base 逻辑。

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

| 技巧                | 原理                                                                                     |
| ------------------- | ---------------------------------------------------------------------------------------- |
| `io.Pipe` 流式传输  | 生产端（Transformer output）和消费端（COPY 协议）通过 pipe 连接，数据不落盘、不缓存全量  |
| 4MB 写缓冲          | `bytes.Buffer` 初始 4MB，累积到 3MB flush 一次，减少 `write()` 系统调用                  |
| COPY goroutine 异步 | COPY 消费在独立 goroutine，生产阻塞即为背压信号                                          |
| CSV 格式约定        | `FORMAT CSV, DELIMITER ',', QUOTE '"', NULL ''`，Transform 层统一输出符合此格式的 string |

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

| 技巧                                   | 原理                                                                                                                                                       |
| -------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| TRUNCATE + COPY 同事务                 | 原子性保证：失败自动回滚，不会出现"清空了但没写入"的中间态                                                                                                 |
| Lock Timeout 退避                      | `SET LOCAL lock_timeout = '30s'`；TRUNCATE 需要 ACCESS EXCLUSIVE 锁，若被阻塞则超时后退化为 `DELETE FROM`（仅需 ROW EXCLUSIVE 锁），避免长时间阻塞其他会话 |
| 延迟启动 `drainFirstBatch()`           | 先从 channel 取第一个非空 batch，若 Reader 无数据直接跳过，不执行无意义的 TRUNCATE                                                                         |
| `CREATE TEMP TABLE ... ON COMMIT DROP` | Full 模式不需要 staging 表，但 Merge/Append 需要——临时表随事务结束自动清理                                                                                 |

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

| 技巧                         | 原理                                                                                     |
| ---------------------------- | ---------------------------------------------------------------------------------------- |
| Staging 中转                 | 先 COPY 到 temp table 再 `INSERT INTO ... SELECT`，比逐行 INSERT 快 1~2 个数量级         |
| Watermark 原子更新           | 水位更新与数据写入在同一事务，保证一致性——不会出现"数据写了但水位没更新"导致重复同步     |
| 分段提交 `commit_batch_size` | 超大表场景，每 N 个 batch 提交一次事务并推进水位；中断后从上次水位续传，避免从头同步     |
| Watermark Fallback 链        | ① 查 `job_data_sync_v2.incr_point` → ② 查目标表 `MAX(incr_field)` → ③ 按字段名推断默认值 |

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

| 技巧                             | 原理                                                                                                                 |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| DELETE + INSERT 代替 ON CONFLICT | 不依赖 UNIQUE 约束，支持任意复合 PK（逗号分隔），兼容 Greenplum 等不完整支持 `ON CONFLICT` 的引擎                    |
| JOIN 条件自动构建                | `buildJoinCondition("t", "s", "pk1,pk2")` → `t.pk1=s.pk1 AND t.pk2=s.pk2`                                            |
| 与 Append 共用核心逻辑           | `writeIncrOnce` / `writeIncrSegmented` 通过 `needDelete bool` 参数区分，最大化代码复用                               |
| 分段模式防死锁                   | `select { case feedCh <- batch; case <-copyDone }` 同时监听两个 channel，COPY goroutine 异常退出时主循环不会永久阻塞 |

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

`GetColumnHandlers()` 根据 `sql.ColumnType.DatabaseTypeName()` 为每一列返回一个 `ColHandler func(any) string`：

- 数字类型 → `fmt.Sprintf`
- 时间类型 → 格式化为 ISO 字符串
- 字节/字符串 → 转义 CSV 特殊字符（逗号、引号、换行）
- NULL → 空字符串（配合 COPY 的 `NULL ''`）

### 4.3 Batch Channel 缓冲

`make(chan RowBatch, 8)` — 8 个 batch 的缓冲平衡 Reader（网络 IO 密集）和 Writer（磁盘 IO 密集）的速度差异。

---

## 5. Transform 层设计

当前为**无状态的列级序列化**：

```go
type Transformer interface {
    Transform(batch RowBatch) CSVBatch
}
```

- Pipeline 内使用 `min(NumCPU, 2)` 个 worker 并发执行 Transform，输出到 `csvChan`（缓冲 4）。
- 设计为接口，未来可扩展为支持字段计算、脱敏、过滤等用户自定义逻辑。

---

## 6. Watermark 机制

### 6.1 存储

水位存储在目标库的 `manager.job_data_sync` 表中，按 (job_name + source identity + target identity) 唯一定位。

### 6.2 读取（启动时）

```
getWatermark()
  → 查 job_data_sync_v2.incr_point
  → 若空：SELECT MAX(incr_field) FROM target_table
  → 若仍空：defaultIncrPoint(field_name)
       含 time/date/cdt/udt → "1970-01-01 00:00:00.000"
       其他                  → "1"
```

### 6.3 写入（提交时）

采用 **Upsert 模式**：先 UPDATE（按匹配键），受影响行数为 0 则 INSERT。保证首次运行和后续运行均可正确写入。

---

## 7. 错误处理策略

| 层级          | 策略                                                                  |
| ------------- | --------------------------------------------------------------------- |
| Task 级       | `error_policy: abort` 立即退出 / `continue` 跳过当前 task             |
| Source 级     | 单个 source 失败后 `continue` 执行同 task 的下一个 source             |
| Pipeline 内部 | Reader 错误 `log.Fatal`（不可恢复）；Writer 错误返回 error 由上层处理 |
| 分段模式      | 每段独立事务，已提交段不回滚；失败段回滚，程序退出后可从水位续传      |

---

## 8. 并发模型总结

```
main goroutine
 ├── Worker Pool (4 goroutines) ── 消费 taskCh
 │    └── per source:
 │         ├── Reader goroutine      ── DB query → rowChan (buf=8)
 │         ├── Transform workers (2) ── rowChan → csvChan (buf=4)
 │         └── Writer (当前 goroutine) ── csvChan → COPY
 │              └── COPY goroutine   ── pipe consumer
 └── wg.Wait()
```

最大并发度 = 4 tasks × (1 reader + 2 transformers + 1 copy) = **16 goroutines**（不含 runtime 内部）。

---

## 9. 扩展点

| 方向             | 当前状态        | 扩展方式                    |
| ---------------- | --------------- | --------------------------- |
| 新数据源         | MSSQL / PG / GP | 实现 `readerDialect` 接口   |
| 新目标库         | 仅 PG/GP        | 实现 `writerDialect` 接口   |
| 自定义 Transform | 仅类型序列化    | 实现 `Transformer` 接口     |
| 监控指标         | log 打印耗时    | 注入 metrics collector      |
| 重试机制         | 无              | 在 `runTask` 外层包装 retry |
| 密码管理         | 明文 YAML       | 对接 Vault / 环境变量替换   |
