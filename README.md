# `db-etl`

一个基于 Go 的数据库 ETL 工具，用于从 `mssql` / `postgres` / `greenplum` 读取数据，经过统一转换后写入目标库。

## 环境准备

```sql

CREATE SCHEMA IF NOT EXISTS manager;

CREATE TABLE IF NOT EXISTS manager.job_data_sync
(
    job_id serial primary key,
    src_schema_name character varying(100) COLLATE pg_catalog."default",
    src_table_name character varying(100) COLLATE pg_catalog."default",
    dst_schema_name character varying(100) COLLATE pg_catalog."default",
    dst_table_name character varying(100) COLLATE pg_catalog."default",
    src_select_statement text COLLATE pg_catalog."default",
    src_where_statement text COLLATE pg_catalog."default",
    sync_mode character varying(10) COLLATE pg_catalog."default",
    src_incr_field character varying(100) COLLATE pg_catalog."default",
    dst_pk character varying(100) COLLATE pg_catalog."default",
    fields_mapping jsonb,
    incr_point text COLLATE pg_catalog."default",
    cdt timestamp without time zone,
    udt timestamp without time zone,
    remark text COLLATE pg_catalog."default",
    inuse boolean,
    src_instance character varying(50) COLLATE pg_catalog."default",
    src_db_name character varying(50) COLLATE pg_catalog."default",
    job_name character varying(50) COLLATE pg_catalog."default",
    src_conn_id integer,
    dst_distributed_by character varying(100) COLLATE pg_catalog."default",
    created_by character varying(50) COLLATE pg_catalog."default",
    modified_by character varying(50) COLLATE pg_catalog."default",
    src_rawsql text COLLATE pg_catalog."default"
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
      - dbname: source-mssql
        batch_size: 10000
        table: dbo.source_table
        incr_field: updated_at
    target:
      dbname: target-pg
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
- `tasks`：任务定义列表。

### `databases`

数据库连接定义列表。

每项字段如下：

| 字段       | 说明                                                      |
| ---------- | --------------------------------------------------------- |
| `name`     | 数据库别名，供 `sources[].dbname` 和 `target.dbname` 引用 |
| `type`     | 数据库类型：`mssql`、`postgres`、`greenplum`              |
| `host`     | 主机地址                                                  |
| `port`     | 端口                                                      |
| `user`     | 用户名                                                    |
| `password` | 密码                                                      |
| `database` | 数据库名                                                  |

### `tasks`

任务定义列表。每个任务将多个 `sources` 的数据写入一个 `target`。

| 字段      | 说明                                                                        |
| --------- | --------------------------------------------------------------------------- |
| `name`    | 任务名称，配置了 `incr_field` 时必填，写入 `manager.job_data_sync.job_name` |
| `type`    | 任务类型，目前支持 `query`                                                  |
| `comment` | 可选备注                                                                    |
| `sources` | 源配置列表，见下节                                                          |
| `target`  | 目标配置，见下节                                                            |

## `sources` 配置

每个 source 表示一个源库读取定义。

| 字段         | 必填   | 说明                                                                             |
| ------------ | ------ | -------------------------------------------------------------------------------- |
| `dbname`     | ✅     | 引用 `databases[].name`                                                          |
| `sql`        | 二选一 | 自定义查询语句                                                                   |
| `table`      | 二选一 | 直接读取的表名，格式 `schema.table`                                              |
| `batch_size` |        | 每批读取行数，默认 `10000`                                                       |
| `incr_field` |        | 增量抽取字段名（日期/时间类型），配合 watermark 实现断点续传                     |
| `incr_point` |        | 增量起点，通常由程序从 watermark 自动回填，也可手动指定初始值                    |
| `order_by`   |        | 查询排序表达式，启用 `commit_batch_size` 时若未指定则自动补为 `<incr_field> ASC` |

### `sql` 与 `table` 规则

- `sql` 和 `table` 至少配置一个，不能同时配置。
- 使用 `table` 时，watermark 的 `src_schema_name` / `src_table_name` 直接从 `table` 解析。
- 使用 `sql` 时，watermark 的源标识使用 SQL 文本本身（写入 `manager.job_data_sync.src_rawsql`）。

### 增量抽取约束

当配置了 `incr_field` 时，必须满足：

- `tasks[].name` 必须配置。
- 启用 `commit_batch_size` 时，查询必须有序（框架自动补 `ORDER BY incr_field ASC`，或手动指定 `order_by`）。

## `target` 配置

| 字段                | 必填         | 说明                                                                                                                       |
| ------------------- | ------------ | -------------------------------------------------------------------------------------------------------------------------- |
| `dbname`            | ✅           | 引用 `databases[].name`                                                                                                    |
| `table`             | ✅           | 目标表，建议使用 `schema.table` 格式                                                                                       |
| `mode`              | ✅           | 写入模式，见下节                                                                                                           |
| `pk`                | merge 时必填 | 主键列列表，多列用逗号分隔，如 `id` 或 `id,tenant_id`                                                                      |
| `commit_batch_size` |              | 分段提交粒度（批次数），`0` 表示整个任务在单个事务中完成（默认）；设置后每 N 个 batch 提交一次事务并更新水位，适用于超大表 |

### 支持的 `mode`

| 值       | 说明                                                          |
| -------- | ------------------------------------------------------------- |
| `copy`   | 直接 COPY 到目标表（全量覆盖写入）                            |
| `merge`  | 先写入临时表，再按 `pk` 做 DELETE + INSERT（支持增量 upsert） |
| `append` | 追加写入（保留，行为以实现为准）                              |
| `full`   | 全量替换（保留，行为以实现为准）                              |

## 示例

### 1. SQL → Target Copy

```yaml
name: etl_copy_demo
tasks:
  - type: query
    sources:
      - dbname: source-mssql
        batch_size: 10000
        sql: "SELECT * FROM [dbo].[source_table]"
    target:
      dbname: target-pg
      table: target_schema.target_table_1
      mode: copy
```

### 2. Table → Target Copy

```yaml
name: etl_table_copy
tasks:
  - type: query
    sources:
      - dbname: source-mssql
        batch_size: 10000
        table: "dbo.source_table_2"
    target:
      dbname: target-pg
      table: target_schema.target_table_2
      mode: copy
```

### 3. Table → Target Merge（增量）

```yaml
name: etl_incremental
tasks:
  - name: order_sync
    type: query
    sources:
      - dbname: source-mssql
        batch_size: 10000
        table: dbo.orders
        incr_field: updated_at
    target:
      dbname: target-pg
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
      - dbname: source-mssql
        batch_size: 10000
        table: dbo.big_table
        incr_field: updated_at
        # order_by 可省略，自动补为 "updated_at ASC"
    target:
      dbname: target-pg
      table: ods.big_table
      mode: merge
      pk: id
      commit_batch_size: 100 # 每 100 批（约 100 万行）提交一次事务
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

## 运行

```bash
# 单次执行（默认）
go run . -config config.yaml

# 后台服务模式（每 5 分钟执行一次）
go run . -config config.yaml -mode server -interval 5m
```

或构建后执行：

```bash
make build
./bin/db-etl-linux-amd64 -config config.yaml
```
