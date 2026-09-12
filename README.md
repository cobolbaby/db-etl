# `db-etl`

一个基于 Go 的数据库 ETL 工具，用于从 `mssql` / `postgres` / `greenplum` / `oracle` 读取数据，经过统一转换后写入目标库。

## 环境准备

```sql

CREATE SCHEMA IF NOT EXISTS manager;

CREATE TABLE IF NOT EXISTS manager.job_data_sync
(
    job_id serial primary key,
    job_name character varying(50) COLLATE pg_catalog."default",
    src_schema_name character varying(100) COLLATE pg_catalog."default",
    src_table_name character varying(100) COLLATE pg_catalog."default",
    src_rawsql text COLLATE pg_catalog."default",
    dst_schema_name character varying(100) COLLATE pg_catalog."default",
    dst_table_name character varying(100) COLLATE pg_catalog."default",
    src_where_statement text COLLATE pg_catalog."default",
    sync_mode character varying(10) COLLATE pg_catalog."default",
    src_incr_field character varying(100) COLLATE pg_catalog."default",
    dst_pk character varying(100) COLLATE pg_catalog."default",
    fields_mapping jsonb,
    incr_point text COLLATE pg_catalog."default",
    cdt timestamp without time zone,
    udt timestamp without time zone,
    created_by character varying(50) COLLATE pg_catalog."default",
    modified_by character varying(50) COLLATE pg_catalog."default",
    remark text COLLATE pg_catalog."default",
    inuse boolean,
    src_conn_name character varying(50) COLLATE pg_catalog."default",
    src_db_name character varying(50) COLLATE pg_catalog."default",
    src_conn_id integer
)
TABLESPACE pg_default;

```

## 配置文件

程序默认从项目根目录读取 `config.yaml`，也可通过 `-config` 参数指定路径。

### 顶层结构

```yaml
name: my_etl_job # 全局任务名，必填
comment: 可选备注
error_policy: abort # abort（默认）或 continue

databases:
  - name: source-mssql
    type: mssql
    host: mssql.example.internal
    port: 1433
    user: <USERNAME>
    password: <PASSWORD>
    database: <DATABASE_NAME>

tasks:
  - name: sample_metric_sync
    type: query
    sources:
      - conn_name: source-mssql
        batch_size: 10000
        table: dbo.source_table
        incr_field: updated_at
    target:
      conn_name: target-pg
      table: target_schema.target_metric
      mode: merge
      pk: pk_col_1,pk_col_2
```

## 字段说明

### 顶层字段

- `name`：全局任务名，必填。写入 watermark 表时作为 `job_name` 的默认值（可被 `tasks[].name` 覆盖）。
- `comment`：可选备注。
- `error_policy`：任务失败策略，`abort`（默认，遇错立即退出）或 `continue`（跳过失败任务继续执行）。
- `databases`：数据库连接定义列表。
- `s3`：对象存储落地端定义列表，供 `target.s3` 引用。
- `meta_db`：存放 `manager.job_data_sync` 的数据库别名（引用 `databases[].name`），作为水位（增量指针）的存储库；未配置时不回写水位。任务来源与水位存储解耦：yaml 中显式定义了 `tasks` 时优先使用 `tasks`，`meta_db` 仅用于回写水位；仅当 yaml 未定义 `tasks` 时，才回退为按 `name` 从 `manager.job_data_sync` 加载任务列表。
- `retry`：失败重试策略（按 source 粒度），见下。
- `tasks`：任务定义列表。

### `retry`

```yaml
retry:
  max_attempts: 3 # 总尝试次数（含首次），默认 3
  delay_seconds: 5 # 初始退避延迟（秒），默认 5
  max_delay_seconds: 60 # 退避延迟上限（秒），默认 60
```

数据错误、约束冲突、SQL 语法错误等不可重试类型会直接失败，不消耗重试次数。

### `databases`

数据库连接定义列表。

| 字段                | 说明                                                                                             |
| ------------------- | ------------------------------------------------------------------------------------------------ |
| `name`              | 数据库别名，供 `sources[].conn_name` 和 `target.conn_name` 引用                                  |
| `id`                | 数据库唯一标识（可选），供 `sources[].conn_id` 和 `target.conn_id` 引用；匹配时优先级高于 `name` |
| `type`              | 数据库类型：`mssql`、`postgres`、`greenplum`、`oracle`（`oracle` 仅支持作为源端）                |
| `host`              | 主机地址                                                                                         |
| `port`              | 端口                                                                                             |
| `user`              | 用户名                                                                                           |
| `password`          | 密码                                                                                             |
| `database`          | 数据库名（Oracle 填 service name）                                                               |
| `ping_timeout`      | 建连探活超时（秒），默认 10 秒                                                                   |
| `lock_timeout`      | PostgreSQL/Greenplum 等锁超时（秒），通过连接串注入会话参数                                      |
| `statement_timeout` | 语句执行超时（秒），0 表示不限制                                                                 |
| `timezone`          | 固定写入端 PostgreSQL 会话时区，如 `UTC`、`+08`                                                  |

### `s3`

对象存储（S3 / MinIO 等 S3 兼容服务）落地端定义列表。

```yaml
s3:
  - name: lake
    format: parquet # 目前仅支持 parquet，留空同义
    endpoint: minio.internal:9000
    bucket: ods
    prefix: db-etl/ # 可选，对象 key 前缀
    access_key: ${S3_ACCESS_KEY}
    secret_key: ${S3_SECRET_KEY}
    use_ssl: false
```

| 字段         | 必填 | 说明                                                             |
| ------------ | ---- | ---------------------------------------------------------------- |
| `name`       | ✅   | 存储别名，供 `target.s3` 引用                                    |
| `format`     |      | 落地格式，目前仅 `parquet`（空值按 `parquet` 处理）              |
| `endpoint`   | ✅   | S3 端点，如 `s3.<region>.amazonaws.com` 或 `minio.internal:9000` |
| `region`     |      | 区域，S3 兼容存储可留空                                          |
| `bucket`     | ✅   | 目标 bucket                                                      |
| `prefix`     |      | 对象 key 前缀                                                    |
| `access_key` |      | 访问密钥，支持 `${ENV}` 引用                                     |
| `secret_key` |      | 私有密钥，支持 `${ENV}` 引用                                     |
| `use_ssl`    |      | 是否走 https，默认 `false`                                       |

对象 key 由 `target.table` 推导，且是确定性的（不含时间戳），便于中断重跑时覆盖同名对象：

- 非增量模式：`<prefix>/<table>.parquet`
- 增量模式：`<prefix>/<table>_<incr_point>.parquet`

其中 `<table>` / `<incr_point>` 会做安全化处理：仅保留字母、数字、`_`、`-`，其余字符（`.`、`/`、空格、`:` 等）替换为 `_`。例如 `ods.orders` → `ods_orders.parquet`。

### `tasks`

任务定义列表。每个任务将多个 `sources` 的数据写入一个 `target`。

| 字段        | 说明                                                                        |
| ----------- | --------------------------------------------------------------------------- |
| `name`      | 任务名称，配置了 `incr_field` 时必填，写入 `manager.job_data_sync.job_name` |
| `type`      | 任务类型，目前支持 `query`                                                  |
| `comment`   | 可选备注                                                                    |
| `sources`   | 源配置列表，见下节                                                          |
| `target`    | 目标配置，见下节                                                            |
| `transform` | 转换链，在写入前对数据做结构重塑，见下节                                    |
| `hooks`     | 前置/后置 SQL hook，见下节                                                  |

## `sources` 配置

每个 source 表示一个源库读取定义。

| 字段              | 必填   | 说明                                                                             |
| ----------------- | ------ | -------------------------------------------------------------------------------- |
| `conn_id`         | 二选一 | 引用 `databases[].id`；与 `conn_name` 至少填一个，优先级高于 `conn_name`         |
| `conn_name`       | 二选一 | 引用 `databases[].name`                                                          |
| `sql`             | 二选一 | 自定义查询语句                                                                   |
| `table`           | 二选一 | 直接读取的表名，格式 `schema.table`；仅 SQL Server 支持 `db.schema.table`        |
| `where_statement` |        | 附加过滤条件；使用 `table` 时直接拼到 `WHERE`，使用 `sql` 时作为外层过滤条件追加 |
| `fields_mapping`  |        | 字段投影/映射；仅支持简单 map，格式为 `源字段或表达式: 目标字段`                 |
| `batch_size`      |        | 每批读取行数，默认 `10000`                                                       |
| `incr_field`      |        | 增量抽取字段名（日期/时间类型），配合 watermark 实现断点续传                     |
| `incr_point`      |        | 增量起点，通常由程序从 watermark 自动回填，也可手动指定初始值                    |
| `order_by`        |        | 查询排序表达式，启用 `commit_batch_size` 时若未指定则自动补为 `<incr_field> ASC` |

### `sql` 与 `table` 规则

- `sql` 和 `table` 至少配置一个，不能同时配置。
- 使用 `table` 时，watermark 的 `src_schema_name` / `src_table_name` 直接从 `table` 解析；只有 SQL Server 源支持三段式 `db.schema.table`。
- 使用 `table` 时，优先保留 `table` 形态，`where_statement` / `fields_mapping` 在 reader 阶段再拼装查询，不会在配置加载阶段改写成 SQL。
- 使用 `sql` 时，watermark 的源标识使用 SQL 文本本身（写入 `manager.job_data_sync.src_rawsql`）。

### `fields_mapping` 源字段的引号规则

`fields_mapping` 的 key（源字段）会按启发式自动加引号：

- **纯列名**（含空格、中文、斜杠等，如 `Analysis Result Judge`、`故障DC/LC`、`Cost Saving`）会自动用方言引号包裹（MSSQL 用 `[]`，PostgreSQL 用 `""`）。
- **SQL 表达式 / 限定名**（含圆括号或点号，如 `GETDATE()`、`CAST(x AS INT)`、`dbo.sourceId`）会原样透传，不加引号。
- **已显式引用的标识符**（如 `[Price(USD)]`、`"OrderID"`）会原样保留，不会二次加引号。

> ⚠️ **含圆括号的列名需手动引用。** 由于 `Price(USD)`（列名）与 `GETDATE()`（函数）在词法上无法区分，含圆括号的列名（如 `Price(USD)`、`等級(Level)`、`故障原廠MPN(Material MPN)`）会被当作表达式而不加引号，导致 SQL 语法错误。这类列名必须在配置中预先自行引用（MSSQL 写成 `[Price(USD)]`，PostgreSQL 写成 `"Price(USD)"`），或改用 `sql` / `src_rawsql` 手写查询。

### 增量抽取约束

当配置了 `incr_field` 时，必须满足：

- `tasks[].name` 必须配置。
- 启用 `commit_batch_size` 时，查询必须有序（框架自动补 `ORDER BY incr_field ASC`，或手动指定 `order_by`）。

## `transform` 配置

`transform` 是 `tasks[]` 下的转换步骤列表，在 reader 之后、writer 之前按顺序执行。每个步骤只能指定一种转换类型，目前支持 `unpivot`。

### `unpivot`（列转行）

把宽表中的一组列展开成 `key + value` 两列多行，其余列作为标识列逐行重复保留。
展开在 ETL 侧完成，源端仍按宽表抽取，避免在源端 SQL 里用 `UNION ALL` 重复标识列导致传输量成倍放大。

```yaml
tasks:
  - name: monthly_plan_sync
    sources:
      - conn_name: source-mssql
        table: dbo.monthly_plan
    transform:
      - unpivot:
          key_field: day # 承载「列标签」的目标列名
          value_field: qty # 承载「列值」的目标列名
          drop_null: true # 源列值为 NULL 时跳过该行，默认 false
          columns: # 源列名 -> 写入 key_field 的标签
            Day1: "1"
            Day2: "2"
            Day3: "3"
    target:
      conn_name: target-pg
      table: ods.monthly_plan_long
      mode: full
```

| 字段          | 必填 | 说明                                                             |
| ------------- | ---- | ---------------------------------------------------------------- |
| `key_field`   | ✅   | 输出中承载列标签的目标列名，仅允许字母/数字/下划线，不得为保留字 |
| `value_field` | ✅   | 输出中承载列值的目标列名，同上，且不能与 `key_field` 相同        |
| `columns`     | ✅   | 「宽表源列名 -> 标签」映射；未出现在映射中的列作为标识列保留     |
| `drop_null`   |      | 为 `true` 时跳过源列值为 NULL 的展开行                           |

> `value_field` 汇聚的是多个异构源列的取值，因此统一以文本输出（时间采用 `2006-01-02 15:04:05.999999999` 格式）。源列为 NULL 时仍保持 NULL。

## `target` 配置

| 字段                | 必填         | 说明                                                                                                                       |
| ------------------- | ------------ | -------------------------------------------------------------------------------------------------------------------------- |
| `conn_id`           | 三选一       | 引用 `databases[].id`；与 `conn_name` 至少填一个，优先级高于 `conn_name`                                                   |
| `conn_name`         | 三选一       | 引用 `databases[].name`                                                                                                    |
| `s3`                | 三选一       | 引用 `s3[].name`，写入对象存储（parquet）；与 `conn_id` / `conn_name` 互斥                                                 |
| `table`             | ✅           | 目标表，建议使用 `schema.table` 格式；写 S3 时用于推导对象 key                                                             |
| `mode`              | ✅           | 写入模式，见下节                                                                                                           |
| `pk`                | merge 时必填 | 主键列列表，多列用逗号分隔，如 `id` 或 `id,tenant_id`                                                                      |
| `commit_batch_size` |              | 分段提交粒度（批次数），`0` 表示整个任务在单个事务中完成（默认）；设置后每 N 个 batch 提交一次事务并更新水位，适用于超大表 |
| `truncate_timeout`  |              | full 模式下 TRUNCATE 等锁超时（秒），默认 10 秒；超时后自动退避为 DELETE FROM                                              |

### 支持的 `mode`

| 值        | 说明                                                                                                                                                             |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `initial` | 首次全量回填：直接 COPY 到目标表，不清空已有数据，仅执行追加写入；上下游会话超时默认放宽至 2 小时（约可覆盖 2 亿行以内的同步），可通过 `statement_timeout` 覆盖  |
| `full`    | 先清空目标表，再将本次抽取结果全量覆盖写入                                                                                                                       |
| `append`  | 与 `initial` 一样追加写入；`incr_field` 可选，配置后按水位增量抽取并在写入后更新水位，不配则每次按 SQL 自身圈定的范围抽取追加                                    |
| `merge`   | 先写入临时表，再按 `pk` 做 DELETE + INSERT（支持增量 upsert）；`incr_field` 可选，配置后按水位增量抽取并更新水位，不配则每次按 SQL 自身圈定的范围全量重抽 upsert |

> `append` 只追加不去重。不配 `incr_field` 时，重跑会产生重复行，需自行在 `sql` 里圈定不重叠的窗口，或由下游查询去重。

> 目标为 `s3` 时，`initial` / `full` 写入单个覆盖式对象，`append` / `merge` 按本次起点水位写入独立对象。
> 对象存储无法按 pk 原地删除/更新，故 `merge` 行为与 `append` 一致，**去重需由下游查询处理**；
> 幂等性由确定性对象 key 覆盖保证。因增量对象 key 取自起点水位，`s3` 目标的 `merge` 同样要求 `incr_field`。

## `hooks` 配置

任务的前置和后置 SQL hook，支持扩展不同类型的 executor。

| 字段   | 说明                           |
| ------ | ------------------------------ |
| `pre`  | 前置 hook 列表，数据同步前执行 |
| `post` | 后置 hook 列表，数据同步后执行 |

每个 hook 配置：

| 字段   | 必填 | 说明                                              |
| ------ | ---- | ------------------------------------------------- |
| `type` |      | hook 类型，默认 `sql`                             |
| `spec` | ✅   | 类型特定的配置，SQL 模式包含 `conn_name` 和 `sql` |

### Hook 配置示例

```yaml
hooks:
  pre:
    - type: sql
      spec:
        conn_name: "src_db"
        sql: "EXEC sp_generate_data"
  post:
    - type: sql
      spec:
        conn_name: "target_pg"
        sql: "ANALYZE public.target_table"
```

## 示例

### 1. SQL → Target Copy

```yaml
name: etl_copy_demo
tasks:
  - type: query
    sources:
      - conn_name: source-mssql
        batch_size: 10000
        sql: "SELECT * FROM [dbo].[source_table]"
    target:
      conn_name: target-pg
      table: target_schema.target_table_1
      mode: initial
```

### 2. Table → Target Copy

```yaml
name: etl_table_copy
tasks:
  - type: query
    sources:
      - conn_name: source-mssql
        batch_size: 10000
        table: "dbo.source_table_2"
        where_statement: "is_deleted = 0"
        fields_mapping:
          source_id: id
          CustomerName: customer_name
          updated_at: updated_at
    target:
      conn_name: target-pg
      table: target_schema.target_table_2
      mode: initial
```

### 3. Table → Target Merge（增量）

```yaml
name: etl_incremental
tasks:
  - name: order_sync
    type: query
    sources:
      - conn_name: source-mssql
        batch_size: 10000
        table: dbo.orders
        incr_field: updated_at
    target:
      conn_name: target-pg
      table: ods.orders
      mode: merge
      pk: order_id
```

### 4. 超大表分段提交

```yaml
name: etl_large_table
tasks:
  - name: big_table_sync
    type: query
    sources:
      - conn_name: source-mssql
        batch_size: 10000
        table: dbo.big_table
        incr_field: updated_at
        # order_by 可省略，自动补为 "updated_at ASC"
    target:
      conn_name: target-pg
      table: ods.big_table
      mode: merge
      pk: id
      commit_batch_size: 100 # 每 100 批（约 100 万行）提交一次事务
```

### 5. 带 Hook 的任务

```yaml
name: etl_with_hooks
tasks:
  - name: order_sync
    type: query
    sources:
      - conn_name: source-mssql
        table: dbo.orders
        incr_field: updated_at
    target:
      conn_name: target-pg
      table: ods.orders
      mode: merge
      pk: order_id
    hooks:
      pre:
        - type: sql
          spec:
            conn_name: "target-pg"
            sql: "DELETE FROM ods.orders WHERE updated_at < now() - interval '7 days'"
      post:
        - type: sql
          spec:
            conn_name: "target-pg"
            sql: "ANALYZE ods.orders"
```

### 6. Table → S3 Parquet

```yaml
s3:
  - name: lake
    endpoint: minio.internal:9000
    bucket: ods
    prefix: db-etl/
    access_key: ${S3_ACCESS_KEY}
    secret_key: ${S3_SECRET_KEY}

tasks:
  - name: orders_to_lake
    type: query
    sources:
      - conn_name: "source-mssql"
        table: "dbo.orders"
        batch_size: 20000
    target:
      s3: "lake" # 引用 s3[].name，与 conn_name 互斥
      table: "ods.orders" # 用于推导对象 key：db-etl/ods_orders.parquet
      mode: full
```

## Watermark 说明

当 source 配置了 `incr_field` 时，程序会读写 `manager.job_data_sync` 表来记录同步进度（水位）。

> **注意**：数据库表中的字段名称维持建表语句原样，不随配置 key 的改名而变化。

### 水位匹配键

| 场景           | 匹配字段                                                                                 |
| -------------- | ---------------------------------------------------------------------------------------- |
| `table` source | `job_name` + `src_schema_name` + `src_table_name` + `dst_schema_name` + `dst_table_name` |
| `sql` source   | `job_name` + `src_rawsql` + `dst_schema_name` + `dst_table_name`                         |

### 字段来源

| 数据库字段                           | 来源                                                                 |
| ------------------------------------ | -------------------------------------------------------------------- |
| `job_name`                           | `tasks[].name`                                                       |
| `src_schema_name` / `src_table_name` | `sources[].table` 解析                                               |
| `src_rawsql`                         | `sources[].sql` 文本                                                 |
| `dst_schema_name` / `dst_table_name` | `target.table` 解析                                                  |
| `incr_point`                         | 程序运行时写入，每段（或整体）提交时更新为当前批次 `MAX(incr_field)` |
| `sync_mode`                          | `target.mode`                                                        |
| `src_incr_field`                     | `sources[].incr_field`                                               |
| `dst_pk`                             | `target.pk`                                                          |

### 水位回填逻辑（启动时）

1. 查 `manager.job_data_sync.incr_point`
2. 若无记录或为空 → 查目标表 `MAX(incr_field)`
3. 若目标表也无数据 → 根据字段名推断兜底值（时间类字段返回 `1970-01-01 00:00:00.000`，其他返回 `1`）

> 水位写回采用「UPDATE 未命中则 INSERT」，不依赖 `ON CONFLICT`（兼容 Greenplum，也不把水位库锁在 PostgreSQL 上）。
> 对应地，`job_data_sync` 上没有唯一约束，**同一任务不得并发执行**，否则可能产生重复水位行。

## 运行

```bash
go run . -config config.yaml

# 查看版本信息
go run . -version
```

或构建后执行：

```bash
make build
./bin/db-etl-linux-amd64 -config config.yaml
```

程序为**单次执行**，跑完所有 task 即退出；周期调度请交给 cron / 定时任务平台。
