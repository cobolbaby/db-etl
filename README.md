# `db-etl`

一个基于 Go 的数据库 ETL 工具，用于从 `mssql` / `postgres` / `greenplum` 读取数据，经过统一转换后写入目标库。

## 配置文件

程序默认从项目根目录读取 `config.yaml`。

### 顶层结构

```yaml
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
      - dbname: source-gp-a
        batch_size: 10000
        sql_name: source_schema.metric_a
        sql: select * from source_schema.source_function()
    target:
      dbname: target-pg
      table: target_schema.target_metric
      mode: merge
      dst_pk: pk_col_1,pk_col_2,pk_col_3
```

## 字段说明

### `databases`

数据库连接定义列表。

每项字段如下：

- `name`：数据库别名，供 `tasks.sources[].dbname` 和 `tasks.target.dbname` 引用。
- `type`：数据库类型，支持 `mssql`、`postgres`、`greenplum`。
- `host`：主机地址。
- `port`：端口。
- `user`：用户名。
- `password`：密码。
- `database`：数据库名。

### `tasks`

任务定义列表。每个任务会把多个 `source` 的数据写入一个 `target`。

每项字段如下：

- `name`：任务名称。
  - 当配置了 `sources[].src_incr_field` 时必填。
  - 该值会写入 watermark 表 `manager.job_data_sync_v2.job_name`。
- `type`：任务类型，目前示例使用 `query`。
- `comment`：可选备注。
- `sources`：源配置列表。
- `target`：目标配置。

## `sources` 配置

每个 source 表示一个源库读取定义。

### 通用字段

- `dbname`：引用 `databases[].name`。
- `batch_size`：当前 source 的批次大小，未配置时默认值为 `10000`。
- `sql`：自定义查询语句。
- `table`：直接读取表名。
- `src_incr_field`：增量抽取字段。
- `incr_point`：增量起点，通常由程序从 watermark 中回填。

### `sql` 与 `table` 规则

- `sql` 和 `table` 至少配置一个。
- `sql` 和 `table` 不能同时配置。
- 如果配置的是 `table`，watermark 的 `src_schema_name` / `src_table_name` 直接从 `table` 解析。
- 如果配置的是 `sql`，必须额外配置 `sql_name`。

### `sql_name`

- 仅在 `sql` source 时使用。
- 格式必须为：`src_schema_name.src_table_name`。
- 用于 watermark 记录中的源标识匹配。

例如：

```yaml
sources:
  - dbname: source-gp-b
    batch_size: 10000
    sql_name: source_schema.metric_b
    sql: select * from source_schema.source_function()
```

### 增量抽取约束

当配置了 `src_incr_field` 时，必须满足以下条件：

- `task.name` 必须配置。
- 如果 source 使用 `sql`，则必须配置 `sql_name`。
- 如果 source 使用 `table`，则必须配置 `table`。

## `target` 配置

目标配置定义了写入位置和写入模式。

字段如下：

- `dbname`：引用 `databases[].name`。
- `table`：目标表，建议使用 `schema.table` 格式。
- `mode`：写入模式。
- `dst_pk`：主键列列表，仅 `merge` 模式需要。

### 支持的 `mode`

- `copy`：直接 COPY 到目标表。
- `merge`：先写入临时表，再按 `dst_pk` 删除旧数据并插入新数据。
- `full` / `append`：已在配置结构中保留，具体行为以实现为准。

## 示例

### 1. SQL -> Target Copy

```yaml
tasks:
  - type: query
    sources:
      - dbname: source-mssql
        batch_size: 10000
        sql: "SELECT * FROM [source_schema].[dbo].[source_table]"
    target:
      dbname: target-pg-test
      table: target_schema.target_table_1
      mode: copy
```

### 2. Table -> Target Copy

```yaml
tasks:
  - type: query
    sources:
      - dbname: source-mssql
        batch_size: 10000
        table: "[source_schema].[dbo].[source_table_2]"
    target:
      dbname: target-pg-test
      table: target_schema.target_table_2
      mode: copy
```

### 3. SQL -> Target Merge

```yaml
tasks:
  - name: sample_metric_sync
    type: query
    sources:
      - dbname: source-gp-a
        batch_size: 10000
        sql_name: source_schema.metric_a
        sql: "select * from source_schema.source_function()"
      - dbname: source-gp-b
        batch_size: 10000
        sql_name: source_schema.metric_b
        sql: "select * from source_schema.source_function()"
      - dbname: source-gp-c
        batch_size: 10000
        sql_name: source_schema.metric_c
        sql: "select * from source_schema.source_function()"
      - dbname: source-gp-d
        batch_size: 10000
        sql_name: source_schema.metric_d
        sql: "select * from source_schema.source_function()"
    target:
      dbname: target-pg
      table: target_schema.target_metric
      mode: merge
      dst_pk: pk_col_1,pk_col_2,pk_col_3
```

## Watermark 说明

当 source 配置了 `src_incr_field` 且 target 使用增量相关流程时，程序会访问 `manager.job_data_sync_v2`。

当前匹配键为：

- `job_name`
- `src_schema_name`
- `src_table_name`
- `dst_schema_name`
- `dst_table_name`

其中：

- `job_name` 来自 `tasks[].name`
- `src_schema_name` / `src_table_name` 来自 `sources[].table` 或 `sources[].sql_name`
- `dst_schema_name` / `dst_table_name` 来自 `target.table`

## 运行

```bash
go run .
```

或构建后执行：

```bash
make build
./bin/db-etl
```
