package config

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

// ============================================================================
// 顶层配置与加载入口
// ----------------------------------------------------------------------------
// Config 是整个 YAML 配置的根；LoadConfig 负责读取 + 解析 + 校验，
// (Config).Validate 串联各分段校验并回填默认值。
// ============================================================================

type Config struct {
	ErrorPolicy string       `yaml:"error_policy"`
	Databases   []DBConfig   `yaml:"databases"`
	S3          []S3Config   `yaml:"s3"`
	Tasks       []TaskConfig `yaml:"tasks"`
	Name        string       `yaml:"name"`
	Comment     string       `yaml:"comment"`
	// MetaDB 指定存放 manager.job_data_sync 配置表的数据库别名（引用 databases[].name）。
	MetaDB string `yaml:"meta_db"`
	// Retry 配置任务失败时的重试策略。
	Retry *RetryPolicy `yaml:"retry"`
}

/*
LoadConfig
读取 YAML 配置
*/
func LoadConfig(path string) (*Config, error) {

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg Config

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	// 解析敏感字段中的环境变量引用
	if err := ResolveSecrets(&cfg); err != nil {
		return nil, err
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

/*
Validate
检查配置合法性
*/
func (c *Config) Validate() error {
	// Name 不能为空
	if strings.TrimSpace(c.Name) == "" {
		return fmt.Errorf("config name is required")
	}

	// ErrorPolicy 只能是 "abort" 或 "continue"，默认为 "abort"
	if c.ErrorPolicy == "" {
		c.ErrorPolicy = ErrorPolicyContinue
	} else if c.ErrorPolicy != ErrorPolicyAbort && c.ErrorPolicy != ErrorPolicyContinue {
		return fmt.Errorf("invalid error_policy: %s", c.ErrorPolicy)
	}

	resolver, err := c.validateDatabases()
	if err != nil {
		return err
	}

	s3Resolver, err := c.validateS3()
	if err != nil {
		return err
	}

	if err := c.validateTasks(resolver, s3Resolver); err != nil {
		return err
	}

	return nil
}

// ============================================================================
// 错误策略与重试
// ============================================================================

// ErrorPolicy constants.
const (
	ErrorPolicyAbort    = "abort"
	ErrorPolicyContinue = "continue"
)

// RetryPolicy configures retry behavior for source-level failures.
type RetryPolicy struct {
	MaxAttempts     int `yaml:"max_attempts"`      // Max attempts including the first. Default 3.
	DelaySeconds    int `yaml:"delay_seconds"`     // Initial delay in seconds. Default 5.
	MaxDelaySeconds int `yaml:"max_delay_seconds"` // Max backoff delay in seconds. Default 60.
}

// ============================================================================
// 数据源连接配置（databases）
// ----------------------------------------------------------------------------
// DBConfig 及其类型/解析器/校验/DSN 构造。读取端与写入端连接均由此描述。
// ============================================================================

type DBType string

const (
	DBTypeMSSQL  DBType = "mssql"
	DBTypePG     DBType = "postgres"
	DBTypeGP     DBType = "greenplum"
	DBTypeOracle DBType = "oracle"
)

var supportedDBTypes = map[DBType]struct{}{
	DBTypeMSSQL:  {},
	DBTypePG:     {},
	DBTypeGP:     {},
	DBTypeOracle: {},
}

// dbTypeAliases 将常见的数据库类型写法归一化为标准值。
var dbTypeAliases = map[string]DBType{
	"mssql":      DBTypeMSSQL,
	"sqlserver":  DBTypeMSSQL,
	"postgres":   DBTypePG,
	"postgresql": DBTypePG,
	"greenplum":  DBTypeGP,
	"oracle":     DBTypeOracle,
}

// DefaultPingTimeout 是探活超时的默认值（秒）。
const DefaultPingTimeout = 10

type DBConfig struct {
	// ID 是数据源的稳定唯一标识（对应 job_data_sync.src_conn_id）。
	// 匹配数据源时 ID 优先级高于 Name；Name 可为空。
	ID       string `yaml:"id"`
	Name     string `yaml:"name"`
	Type     DBType `yaml:"type"` // mssql / pg
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	// StatementTimeout 单条语句的执行超时（秒），0 表示不限制（默认）。
	// 写入端（PostgreSQL/Greenplum）通过会话参数 statement_timeout 由服务端强制中断；
	// 读取端（MSSQL / PostgreSQL）则以此为客户端 context 超时上限。
	// merge 模式下 DELETE+INSERT 、大表全量顺扫耗时较长，建议设为 0（不限制）或足够大的值。
	StatementTimeout int `yaml:"statement_timeout"`
	// LockTimeout PostgreSQL/Greenplum 会话的“等锁”超时（秒）。
	// 仅对 PostgreSQL/Greenplum 生效，通过连接串注入会话参数 lock_timeout：
	// 一旦所需的表锁被 DDL / 长事务持有并阻塞，到时立即以 55P03（lock_not_available）
	// 失败并交由重试机制处理，从而避免在 statement_timeout=0（允许长时间顺扫）时被锁死而无限干等。
	// 该参数在建连时注入，读取端（source）与写入端（target）连接均生效，与同步模式无关。
	// <=0 时不注入，保持服务端默认。
	LockTimeout int `yaml:"lock_timeout"`
	// TimeZone 声明本数据源的时区标签（IANA 名称如 "America/Mexico_City"、"Asia/Shanghai"，或 "UTC"），有两处用途：
	//  1. 写入端 PostgreSQL/Greenplum：注入会话时区（-c TimeZone=），使无时区时间字符串写入
	//     timestamptz 列时被确定性解析，不随运行环境（PGTZ/TZ/服务端默认）漂移。
	//  2. 读取端：把无时区时间列（如 SQL Server DATETIME2、PostgreSQL TIMESTAMP）的墙钟
	//     解释为该时区，从而在落地为绝对时刻（如 parquet 归一到 UTC）时携带正确的偏移。
	//     运行节点与数据库节点时区可能不同，故必须显式声明而非依赖运行节点本地时区。
	// 为空时：写入端不注入会话时区（保持服务端默认）；读取端回退到运行节点本地时区（time.Local）。
	TimeZone string `yaml:"timezone"`
	// PingTimeout 建连时探活超时（秒），默认 10 秒。
	// 仅在 reader/hook 工厂的主动探活场景生效（writer 的 pgx.Connect 不走此参数）。
	// 0 或负值时使用内置默认值 10 秒。
	PingTimeout int `yaml:"ping_timeout"`
}

// Location 将 TimeZone 解析为 *time.Location，供读取端把无时区时间列的墙钟解释为该时区。
// 支持 IANA 名称（如 "Asia/Shanghai"）与 "UTC"。TimeZone 为空时回退到运行节点本地时区（time.Local）。
func (db DBConfig) Location() (*time.Location, error) {
	return parseTimeZone(db.TimeZone)
}

// parseTimeZone 解析 IANA 时区名（或 "UTC"）为 *time.Location。空串回退 time.Local。
func parseTimeZone(tz string) (*time.Location, error) {
	tz = strings.TrimSpace(tz)
	if tz == "" {
		return time.Local, nil
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("invalid timezone %q: %w", tz, err)
	}
	return loc, nil
}

// DBResolver 根据 conn_id（优先）或 name 解析数据库配置。
// ID 优先级高于 Name，Name 可为空。
type DBResolver struct {
	byID   map[string]DBConfig
	byName map[string]DBConfig
}

// NewDBResolver 基于 databases 列表构建解析器。
func NewDBResolver(dbs []DBConfig) DBResolver {
	r := DBResolver{
		byID:   make(map[string]DBConfig, len(dbs)),
		byName: make(map[string]DBConfig, len(dbs)),
	}
	for _, db := range dbs {
		if id := strings.TrimSpace(db.ID); id != "" {
			r.byID[id] = db
		}
		if name := strings.TrimSpace(db.Name); name != "" {
			r.byName[name] = db
		}
	}
	return r
}

// Resolve 优先按 connID 匹配，其次按 connName 匹配；都命中不了时返回 false。
// 返回值副本而非指针，调用方的修改不会篡改 resolver 内部持有的共享配置。
func (r DBResolver) Resolve(connID, connName string) (DBConfig, bool) {
	if id := strings.TrimSpace(connID); id != "" {
		if db, ok := r.byID[id]; ok {
			return db, true
		}
	}
	if name := strings.TrimSpace(connName); name != "" {
		if db, ok := r.byName[name]; ok {
			return db, true
		}
	}
	return DBConfig{}, false
}

func (c *Config) validateDatabases() (DBResolver, error) {
	seenIDs := make(map[string]struct{}, len(c.Databases))
	seenNames := make(map[string]struct{}, len(c.Databases))
	for i := range c.Databases {
		db := &c.Databases[i]
		id := strings.TrimSpace(db.ID)
		name := strings.TrimSpace(db.Name)
		// ID 优先，Name 可为空；但至少要有一个作为标识。
		if id == "" && name == "" {
			return DBResolver{}, fmt.Errorf("database id or name required")
		}
		ident := id
		if ident == "" {
			ident = name
		}
		if db.Type == "" {
			return DBResolver{}, fmt.Errorf("database type required for %s", ident)
		}
		db.Type = DBType(strings.ToLower(strings.TrimSpace(string(db.Type))))
		if canonical, ok := dbTypeAliases[string(db.Type)]; ok {
			db.Type = canonical
		}
		if _, ok := supportedDBTypes[db.Type]; !ok {
			return DBResolver{}, fmt.Errorf("unsupported database type %q for %s", db.Type, ident)
		}
		if strings.TrimSpace(db.Database) == "" {
			return DBResolver{}, fmt.Errorf("database is required for %s", ident)
		}
		// PingTimeout 未配置时使用默认值
		if db.PingTimeout == 0 {
			db.PingTimeout = DefaultPingTimeout
		}
		if id != "" {
			if _, dup := seenIDs[id]; dup {
				return DBResolver{}, fmt.Errorf("duplicate database id %q", id)
			}
			seenIDs[id] = struct{}{}
		}
		if name != "" {
			if _, dup := seenNames[name]; dup {
				return DBResolver{}, fmt.Errorf("duplicate database name %q", name)
			}
			seenNames[name] = struct{}{}
		}
	}
	return NewDBResolver(c.Databases), nil
}

/*
DSN
返回数据库连接字符串。

对 PostgreSQL/Greenplum（读取端与写入端连接均使用本函数），在基础连接串上按需追加会话级调优：
  - lock_timeout：被表锁阻塞时快速失败（55P03），交由重试机制处理，避免无限干等；
  - statement_timeout：给大表全量顺扫 / 写入设置执行上限；

两者均仅在 DBConfig 对应字段 >0 时才注入；都未配置时不追加任何 options，保持服务端默认。
*/
func (db *DBConfig) DSN() string {

	switch db.Type {

	case DBTypeMSSQL:
		// encrypt=disable：客户端声明不支持加密，登录包也不走 TLS。
		//   旧版 SQL Server 的 TLS 栈可能有问题：即使用 tlsmin=1.0 放宽版本，
		//   握手阶段仍会被服务器直接重置（read: connection reset by peer）。
		//   disable 彻底跳过 TLS，交由内网链路保证安全。
		return fmt.Sprintf(
			"server=%s;user id=%s;password=%s;port=%d;database=%s;encrypt=disable",
			db.Host,
			db.User,
			db.Password,
			db.Port,
			db.Database,
		)

	case DBTypePG, DBTypeGP:
		base := fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			db.Host,
			db.Port,
			db.User,
			db.Password,
			db.Database,
		)

		var opts []string
		if db.LockTimeout > 0 {
			opts = append(opts, fmt.Sprintf("-c lock_timeout=%d", db.LockTimeout*1000))
		}
		if db.StatementTimeout > 0 {
			opts = append(opts, fmt.Sprintf("-c statement_timeout=%d", db.StatementTimeout*1000))
		}
		// 固定会话时区，确保时间字符串写入 timestamptz 列时被确定性解析。
		if db.TimeZone != "" {
			opts = append(opts, fmt.Sprintf("-c TimeZone=%s", db.TimeZone))
		}
		if len(opts) == 0 {
			return base
		}
		// libpq options 值含空格，须用单引号包裹；pgx 按 libpq 规则解析。
		return base + fmt.Sprintf(" options='%s'", strings.Join(opts, " "))

	case DBTypeOracle:
		// go-ora 采用 URL 形式的连接串：oracle://user:password@host:port/service_name。
		// database 字段承载 Oracle 的 service name（或 SID）。
		// 用 net/url 构造可对用户名/口令中的特殊字符做百分号转义，避免拼串注入与解析歧义。
		u := &url.URL{
			Scheme: "oracle",
			User:   url.UserPassword(db.User, db.Password),
			Host:   fmt.Sprintf("%s:%d", db.Host, db.Port),
			Path:   "/" + strings.TrimPrefix(db.Database, "/"),
		}
		return u.String()

	default:
		panic("unsupported db type: " + db.Type)

	}
}

// Driver 返回数据库连接所使用的 driver 名称。
func (db *DBConfig) Driver() string {
	switch db.Type {
	case DBTypeMSSQL:
		return "sqlserver"
	case DBTypePG, DBTypeGP:
		return "pgx"
	case DBTypeOracle:
		return "oracle"
	default:
		panic("unsupported db type: " + db.Type)
	}
}

// ============================================================================
// 对象存储配置（s3）
// ----------------------------------------------------------------------------
// S3Config 描述 parquet-on-S3 落地端；S3Resolver 供 target 通过 name 引用解析。
// ============================================================================

// S3Format 标识落地文件格式。目前仅支持 parquet。
type S3Format string

const (
	S3FormatParquet S3Format = "parquet"
)

// S3Config 描述一个 S3 / S3 兼容对象存储落地端（含 MinIO）。
// target 通过 s3 字段引用 s3[].name，与 conn_id/conn_name 互斥。
// AccessKey / SecretKey 支持 ${ENV} 引用，由 ResolveSecrets 解析。
type S3Config struct {
	Name      string   `yaml:"name"`
	Format    S3Format `yaml:"format"`     // 目前仅 parquet，空值按 parquet 处理
	Endpoint  string   `yaml:"endpoint"`   // S3 端点，如 s3.<region>.amazonaws.com 或 minio.internal:9000
	Region    string   `yaml:"region"`     // 区域，S3 兼容存储可留空
	Bucket    string   `yaml:"bucket"`     // 目标 bucket（必填）
	Prefix    string   `yaml:"prefix"`     // 对象 key 前缀，可为空
	AccessKey string   `yaml:"access_key"` // 访问密钥（支持 ${ENV}）
	SecretKey string   `yaml:"secret_key"` // 私有密钥（支持 ${ENV}）
	UseSSL    bool     `yaml:"use_ssl"`    // 是否走 https
}

// Validate 校验 s3 存储配置的完整性。
func (s S3Config) Validate() error {
	if strings.TrimSpace(s.Name) == "" {
		return fmt.Errorf("s3 storage name is required")
	}
	if s.Format != "" && s.Format != S3FormatParquet {
		return fmt.Errorf("unsupported s3 format %q for storage %q", s.Format, s.Name)
	}
	if strings.TrimSpace(s.Endpoint) == "" {
		return fmt.Errorf("s3 storage %q: endpoint is required", s.Name)
	}
	if strings.TrimSpace(s.Bucket) == "" {
		return fmt.Errorf("s3 storage %q: bucket is required", s.Name)
	}
	return nil
}

// S3Resolver 按 name 解析 S3Config。
type S3Resolver struct {
	byName map[string]S3Config
}

// NewS3Resolver 基于 s3 列表构建解析器。
func NewS3Resolver(list []S3Config) S3Resolver {
	r := S3Resolver{byName: make(map[string]S3Config, len(list))}
	for _, s := range list {
		if name := strings.TrimSpace(s.Name); name != "" {
			r.byName[name] = s
		}
	}
	return r
}

// Resolve 按 name 匹配 s3 存储，命中不了时返回 false。
func (r S3Resolver) Resolve(name string) (S3Config, bool) {
	s, ok := r.byName[strings.TrimSpace(name)]
	return s, ok
}

// validateS3 校验 s3 段并构建 S3Resolver。
func (c *Config) validateS3() (S3Resolver, error) {
	seen := make(map[string]struct{}, len(c.S3))
	for i := range c.S3 {
		s := &c.S3[i]
		name := strings.TrimSpace(s.Name)
		// Format 默认 parquet。
		if s.Format == "" {
			s.Format = S3FormatParquet
		}
		if err := s.Validate(); err != nil {
			return S3Resolver{}, err
		}
		if _, dup := seen[name]; dup {
			return S3Resolver{}, fmt.Errorf("duplicate s3 storage name %q", name)
		}
		seen[name] = struct{}{}
	}
	return NewS3Resolver(c.S3), nil
}

// ============================================================================
// 任务配置（tasks）
// ----------------------------------------------------------------------------
// TaskConfig 聚合 source/target/transform/hooks；(TaskConfig).Validate 逐项分派到各分段校验。
// ============================================================================

type TaskType string

const (
	TaskTypeEtl  TaskType = "query"
	TaskTypeExec TaskType = "exec"
)

type TaskConfig struct {
	Name    string          `yaml:"name"`
	Type    TaskType        `yaml:"type"`
	Sources []*SourceConfig `yaml:"sources"`
	Target  *TargetConfig   `yaml:"target"`
	Hooks   *Hooks          `yaml:"hooks"`
	// Transform 定义抽取后的数据转换链（在 reader 之后、writer 之前执行）。
	// 数据默认逐列透传；列表中的每个步骤按顺序在其后叠加应用。
	Transform []*TransformConfig `yaml:"transform"`
}

func (c *Config) validateTasks(resolver DBResolver, s3Resolver S3Resolver) error {

	if len(c.Tasks) == 0 && c.MetaDB == "" {
		return fmt.Errorf("tasks cannot be empty (set 'meta_db' to load tasks from database)")
	}

	for _, t := range c.Tasks {
		if err := t.Validate(resolver, s3Resolver); err != nil {
			return err
		}
	}

	return nil
}

func (task TaskConfig) Validate(resolver DBResolver, s3Resolver S3Resolver) error {
	if task.Target == nil {
		return fmt.Errorf("target must be specified")
	}

	for _, source := range task.Sources {
		if err := source.Validate(resolver, task.Target); err != nil {
			return err
		}
	}

	for i, tf := range task.Transform {
		if err := tf.Validate(); err != nil {
			return fmt.Errorf("transform[%d]: %w", i, err)
		}
	}

	if err := task.Target.Validate(resolver, s3Resolver); err != nil {
		return err
	}

	if err := task.Hooks.Validate(resolver); err != nil {
		return err
	}
	return nil
}

// ============================================================================
// 源端配置（source）
// ----------------------------------------------------------------------------
// SourceConfig 描述一次抽取的源库/表/SQL/增量参数；(SourceConfig).Validate 解析数据源并回填默认值。
// ============================================================================

type SourceConfig struct {
	ConnID         string        `yaml:"conn_id"`   // 优先按 conn_id 匹配数据源，为空时回退到 conn_name
	ConnName       string        `yaml:"conn_name"` // 引用 databases[].name
	SQL            string        `yaml:"sql"`
	Table          string        `yaml:"table"`           // SQL 和 Table 至少要指定一个，SQL 优先级更高
	FieldsMapping  FieldsMapping `yaml:"fields_mapping"`  // Table 场景下的字段映射，格式为 map[源字段/表达式]目标字段
	WhereStatement string        `yaml:"where_statement"` // Table 场景下的附加过滤条件；SQL 场景下会作为外层过滤条件追加
	BatchSize      int           `yaml:"batch_size"`
	DBType         DBType        `yaml:"-"`
	Database       string        `yaml:"-"` // 解析数据源后填充，作为 watermark 的 src_db_name
	Mode           ModeType      `yaml:"-"`
	IncrField      string        `yaml:"incr_field"` // 用于增量抽取，指定一个日期/时间字段，配合 Watermark 实现增量抽取
	IncrPoint      string        `yaml:"incr_point"` // 增量抽取的起点
	OrderBy        string        `yaml:"order_by"`   // OrderBy 指定查询排序字段。当 target.commit_batch_size > 0 时必须有序，框架会自动设为 src_incr_field，也可手动指定其他表达式（如 "id ASC"）。
}

func (s *SourceConfig) normalize() {
	s.WhereStatement = strings.TrimSpace(s.WhereStatement)
}

// ValidateTableName 校验表名（source 与 target 共用）：
//  1. 仅允许字母、数字、'_' 与 '.'（'.' 用于区隔 schema.table），其余字符（'/'、空格、':' 等）一律非法；
//  2. 允许 table / schema.table；三段式 db.schema.table 仅 MSSQL 合法。
//
// 空表名返回 nil（source 在 SQL 模式下 table 可为空；target 的非空约束由调用方单独保证）。
func ValidateTableName(table string, dbType DBType) error {
	trimmed := strings.TrimSpace(table)
	if trimmed == "" {
		return nil
	}

	for _, r := range trimmed {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '_', r == '.':
			// 合法字符
		default:
			return fmt.Errorf("table %q contains illegal character %q; only letters, digits, '_' and '.' are allowed", trimmed, r)
		}
	}

	parts := strings.Split(trimmed, ".")
	switch len(parts) {
	case 1, 2:
		return nil
	case 3:
		if dbType != DBTypeMSSQL {
			return fmt.Errorf("table %q uses db.schema.table, which is only supported for mssql", trimmed)
		}
		return nil
	default:
		return fmt.Errorf("table must be table, schema.table, or db.schema.table")
	}
}

func (source *SourceConfig) Validate(resolver DBResolver, target *TargetConfig) error {
	if source == nil {
		return fmt.Errorf("source must be specified")
	}
	if source.ConnID == "" && source.ConnName == "" {
		return fmt.Errorf("source conn_id or conn_name is required")
	}

	srcDB, ok := resolver.Resolve(source.ConnID, source.ConnName)
	if !ok {
		return fmt.Errorf("source db not found (conn_id=%q conn_name=%q)", source.ConnID, source.ConnName)
	}

	source.DBType = srcDB.Type
	source.Database = srcDB.Database

	if source.SQL == "" && source.Table == "" {
		return fmt.Errorf("sql or table must be specified")
	}

	if source.SQL != "" && source.Table != "" {
		return fmt.Errorf("sql and table cannot both be specified")
	}

	if err := ValidateTableName(source.Table, source.DBType); err != nil {
		return err
	}

	if err := source.FieldsMapping.Validate(); err != nil {
		return err
	}

	if source.BatchSize <= 0 {
		source.BatchSize = 10000
	}

	// TODO: 待讨论，暂时将该验证注释掉，允许用户在 append 模式下不配置 incr_field，使用全量抽取的方式。
	// // append 只追加不去重，没有 incr_field 就无从界定增量区间，重跑必然产生重复行，故必填。
	// if target.Mode == ModeTypeAppend && strings.TrimSpace(source.IncrField) == "" {
	// 	return fmt.Errorf("incr_field is required for %s mode", target.Mode)
	// }

	// merge 写 DB 时按 pk 做 DELETE + INSERT，重复抽取同一区间是幂等的，
	// 允许不配 incr_field（由 SQL 自身圈定滚动窗口，如 date >= now() - interval '3 days'）。
	// 但 merge 写对象存储时，增量对象的 key 由起点水位命名（见 incrementalObjectKey），
	// 缺少 incr_field 会让每次增量都覆盖同一个对象，故此场景仍必填。
	if target.Mode == ModeTypeMerge && strings.TrimSpace(target.S3) != "" && strings.TrimSpace(source.IncrField) == "" {
		return fmt.Errorf("incr_field is required for %s mode when target is s3", target.Mode)
	}

	if target.CommitBatchSize > 0 {
		if source.IncrField == "" {
			return fmt.Errorf("incr_field is required when commit_batch_size > 0")
		}
		if source.OrderBy == "" {
			// 用占位符而非裸字段名：交由 reader 的占位符替换按方言加引号，
			// 避免含空格/中文的字段名（如 "Test Finish Date"）拼进 ORDER BY 触发语法错误。
			source.OrderBy = "${SRC_INCR_FIELD} ASC"
		}
	}

	return nil
}

// ============================================================================
// 目标端配置（target）
// ----------------------------------------------------------------------------
// TargetConfig 描述写入端（DB 或 s3）与同步模式；(TargetConfig).Validate 做互斥/主键/表名等校验。
// ============================================================================

type ModeType string

const (
	ModeTypeInitial ModeType = "initial"
	ModeTypeFull    ModeType = "full"
	ModeTypeAppend  ModeType = "append"
	ModeTypeMerge   ModeType = "merge"
)

var supportedTargetModes = map[ModeType]struct{}{
	ModeTypeInitial: {},
	ModeTypeFull:    {},
	ModeTypeAppend:  {},
	ModeTypeMerge:   {},
}

// InitialModeDefaultTimeoutSec 是 initial（首次全量）模式下，源端查询与目标端写入
// 会话超时的默认秒数（2 小时）。该模式用于一次性回填历史数据，单次可能同步上亿行，
// 顺扫与 COPY 写入耗时较长，因此在未显式配置 statement_timeout 时
// 采用较宽松的默认，避免误触发超时中断。经验上 2 小时可覆盖 2 亿行以内的同步。
const InitialModeDefaultTimeoutSec = 7200

// defaultTruncateTimeoutSec 是 full 模式下 TRUNCATE 等锁超时的默认值（秒）。
const defaultTruncateTimeoutSec = 10

// defaultMaxRowsPerRowGroup 是写 parquet 时单个 row group 的默认最大行数。
// 按目标 row group ~128MB / 典型单行大小换算的经验值，避免单个对象退化为单个
// 超大 row group（峰值内存随总行数无限增长、下游难以并行读）。
const defaultMaxRowsPerRowGroup = 200000

type TargetConfig struct {
	ConnID    string   `yaml:"conn_id"`   // 优先按 conn_id 匹配数据源，为空时回退到 conn_name
	ConnName  string   `yaml:"conn_name"` // 引用 databases[].name
	S3        string   `yaml:"s3"`        // 引用 s3[].name，与 conn_id/conn_name 互斥
	Table     string   `yaml:"table"`
	Mode      ModeType `yaml:"mode"`
	IncrField string
	PK        string `yaml:"pk"`
	// CommitBatchSize 控制 merge 模式下每隔多少个 batch 提交一次事务并更新水位。
	// 0 表示不分段，整个任务在单个事务中完成（原有行为）。
	// 适用于超大表，设置后可在中断重启后从上次水位断点续传。
	CommitBatchSize int `yaml:"commit_batch_size"`
	// MaxRowsPerRowGroup 控制写 parquet 时单个 row group 的最大行数（仅对象存储目标生效）。
	// 未配置（0）时回填默认值 defaultMaxRowsPerRowGroup（见 (TargetConfig).Validate）：
	// 将对象内部按行数切分多个 row group，降低峰值内存并提升下游读取并行度。
	// 显式设为负值可关闭切分（整个对象为单个 row group，须等 Close 写 footer 时才落盘）。
	MaxRowsPerRowGroup int64 `yaml:"max_rows_per_row_group"`
	// TruncateTimeout full 模式下 TRUNCATE 尝试获取锁的超时时间（秒）。
	// 超时后自动退避为 DELETE FROM，以避免长时间阻塞下游。
	// 0 表示使用默认值（defaultTruncateTimeoutSec 秒）。
	TruncateTimeout int `yaml:"truncate_timeout"`
}

func (target *TargetConfig) Validate(resolver DBResolver, s3Resolver S3Resolver) error {
	hasDB := strings.TrimSpace(target.ConnID) != "" || strings.TrimSpace(target.ConnName) != ""
	hasS3 := strings.TrimSpace(target.S3) != ""
	if hasDB && hasS3 {
		return fmt.Errorf("target cannot set both s3 and conn_id/conn_name")
	}
	if !hasDB && !hasS3 {
		return fmt.Errorf("target conn_id/conn_name or s3 is required")
	}
	if strings.TrimSpace(target.Table) == "" {
		return fmt.Errorf("target table is required")
	}

	// 先解析目标端，拿到 dbType 供表名校验判定段数（s3 目标无 dbType，三段式表名一律非法）。
	var dbType DBType
	if hasS3 {
		if _, ok := s3Resolver.Resolve(target.S3); !ok {
			return fmt.Errorf("target s3 %q not found", target.S3)
		}
	} else {
		db, ok := resolver.Resolve(target.ConnID, target.ConnName)
		if !ok {
			return fmt.Errorf("target db not found (conn_id=%q conn_name=%q)", target.ConnID, target.ConnName)
		}
		dbType = db.Type
	}

	// 表名会用于对象 key 拼接（s3，见 writer.defaultObjectKey / incrementalObjectKey）
	// 及 SQL 标识符引用（DB），非法字符会破坏 key 结构或引发标识符歧义，
	// 必须在加载阶段 fail-fast，而非在写入端静默替换掩盖配置错误。
	if err := ValidateTableName(target.Table, dbType); err != nil {
		return err
	}
	if _, ok := supportedTargetModes[target.Mode]; !ok {
		return fmt.Errorf("unsupported target mode: %s", target.Mode)
	}
	// merge 模式依赖主键做 DELETE + INSERT（DB）或供下游去重（对象存储），
	// pk 为空属结构性配置错误，必须在加载阶段就 fail-fast。
	if target.Mode == ModeTypeMerge && strings.TrimSpace(target.PK) == "" {
		return fmt.Errorf("pk is required for merge mode (target table %q)", target.Table)
	}

	// s3 目标：回填对象存储相关默认值。
	if hasS3 {
		// row group 行数：未配置（0）回填默认值；显式负值表示关闭切分（归一为 0 交给写入端跳过）。
		if target.MaxRowsPerRowGroup <= 0 {
			target.MaxRowsPerRowGroup = defaultMaxRowsPerRowGroup
		}
	}

	// DB 目标：回填 DB 相关默认值。
	if hasDB {
		// TruncateTimeout 未配置时使用默认值。
		if target.TruncateTimeout == 0 {
			target.TruncateTimeout = defaultTruncateTimeoutSec
		}
	}
	return nil
}

// ============================================================================
// Hook 配置
// ----------------------------------------------------------------------------
// 任务的前置/后置 SQL hook；(Hooks).Validate 校验类型与 conn/sql 必填项。
// ============================================================================

type HookType string

const (
	HookTypeSQL HookType = "sql"
)

// Hooks 定义任务的前置和后置 SQL hook。
type Hooks struct {
	Pre  []HookConfig `yaml:"pre"`
	Post []HookConfig `yaml:"post"`
}

// HookConfig 定义单个 hook。
type HookConfig struct {
	// Type hook 类型，默认 sql。
	Type HookType `yaml:"type"`
	// Spec 类型特定的配置。
	// SQL 模式：{ "conn_name": "xxx", "sql": "SELECT 1" }
	Spec map[string]any `yaml:"spec"`
}

func (hooks *Hooks) Validate(resolver DBResolver) error {
	if hooks == nil {
		return nil
	}

	for i, h := range hooks.Pre {
		if h.Type == "" {
			h.Type = HookTypeSQL
		}
		if h.Type != HookTypeSQL {
			return fmt.Errorf("hooks.pre[%d]: unsupported hook type %q", i, h.Type)
		}
		connName, _ := h.Spec["conn_name"].(string)
		sql, _ := h.Spec["sql"].(string)
		if strings.TrimSpace(connName) == "" {
			return fmt.Errorf("hooks.pre[%d].spec.conn_name is required", i)
		}
		if strings.TrimSpace(sql) == "" {
			return fmt.Errorf("hooks.pre[%d].spec.sql is empty", i)
		}
		if _, ok := resolver.Resolve("", connName); !ok {
			return fmt.Errorf("hooks.pre[%d].spec.conn_name %q not found", i, connName)
		}
	}
	for i, h := range hooks.Post {
		if h.Type == "" {
			h.Type = HookTypeSQL
		}
		if h.Type != HookTypeSQL {
			return fmt.Errorf("hooks.post[%d]: unsupported hook type %q", i, h.Type)
		}
		connName, _ := h.Spec["conn_name"].(string)
		sql, _ := h.Spec["sql"].(string)
		if strings.TrimSpace(connName) == "" {
			return fmt.Errorf("hooks.post[%d].spec.conn_name is required", i)
		}
		if strings.TrimSpace(sql) == "" {
			return fmt.Errorf("hooks.post[%d].spec.sql is empty", i)
		}
		if _, ok := resolver.Resolve("", connName); !ok {
			return fmt.Errorf("hooks.post[%d].spec.conn_name %q not found", i, connName)
		}
	}
	return nil
}

// ============================================================================
// 转换链配置（transform / unpivot）
// ----------------------------------------------------------------------------
// 抽取后、写入前的数据转换步骤。目前仅支持 unpivot（宽表转长表）。
// ============================================================================

// TransformConfig 描述 transform 链中的单个转换步骤。每个步骤只应配置一种转换类型。
type TransformConfig struct {
	// Unpivot 配置列转行（宽表转长表）。
	Unpivot *UnpivotConfig `yaml:"unpivot"`
}

// Validate 校验单个转换步骤的配置。
func (t *TransformConfig) Validate() error {
	if t == nil {
		return fmt.Errorf("transform step must not be empty")
	}
	if t.Unpivot == nil {
		return fmt.Errorf("transform step must specify a transform type (e.g. unpivot)")
	}
	return t.Unpivot.Validate()
}

// UnpivotConfig 配置列转行（unpivot）：把宽表中的一组列展开成两列多行。
// 配置后，源端仍抽取宽表（读取量小），在转换阶段将指定的多列展开为两列
// （key + value）的多行，避免在源端 SQL 中重复标识列导致源库→ETL 网络传输量成倍放大。
// 例如 Day1..Day31 展开为 (day, value)，其余列作为标识列在每行重复保留。
type UnpivotConfig struct {
	// KeyField 输出中承载“列标签”的目标列名（如 "day"）。
	KeyField string `yaml:"key_field"`
	// ValueField 输出中承载“列值”的目标列名（如 "value"）。
	ValueField string `yaml:"value_field"`
	// Columns 是「宽表源列名 -> 写入 KeyField 的标签」映射。
	// 例如 {Day1: "1", Day2: "2", ...}。未出现在此映射中的列作为标识列保留。
	Columns map[string]string `yaml:"columns"`
	// DropNull 为 true 时，源列值为 NULL 的展开行会被跳过（不写入目标）。
	DropNull bool `yaml:"drop_null"`
}

// Validate 校验 unpivot 配置的完整性与字段名合法性。
func (u *UnpivotConfig) Validate() error {
	if u == nil {
		return nil
	}
	key := strings.TrimSpace(u.KeyField)
	value := strings.TrimSpace(u.ValueField)
	if key == "" || value == "" {
		return fmt.Errorf("unpivot key_field and value_field are required")
	}
	for _, field := range []string{key, value} {
		if !isAllowedFieldName(field) {
			return fmt.Errorf("unpivot field %q contains invalid characters; only letters, digits, and underscore are allowed", field)
		}
		if isReservedKeyword(field) {
			return fmt.Errorf("unpivot field %q is a reserved keyword", field)
		}
	}
	if key == value {
		return fmt.Errorf("unpivot key_field and value_field must differ")
	}
	if len(u.Columns) == 0 {
		return fmt.Errorf("unpivot columns must not be empty")
	}
	for source, label := range u.Columns {
		if strings.TrimSpace(source) == "" {
			return fmt.Errorf("unpivot columns contains an empty source column name")
		}
		if strings.TrimSpace(label) == "" {
			return fmt.Errorf("unpivot label for source column %q is empty", source)
		}
	}
	return nil
}

// ============================================================================
// 字段映射（fields_mapping）
// ----------------------------------------------------------------------------
// Table 场景下「源字段/表达式 -> 目标列」的映射，负责投影 SQL 生成与列名解析。
// ============================================================================

type FieldsMapping struct {
	Items map[string]string
}

func (m *FieldsMapping) UnmarshalYAML(value *yaml.Node) error {
	if m == nil {
		return nil
	}

	var parsed map[string]string
	if err := value.Decode(&parsed); err != nil {
		return err
	}

	*m = FieldsMapping{Items: normalizeFieldsMappingItems(parsed)}
	return nil
}

func ParseFieldsMapping(raw string) (FieldsMapping, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return FieldsMapping{}, nil
	}

	var parsed map[string]string
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return FieldsMapping{}, fmt.Errorf("fields_mapping must be json object map[string]string: %w", err)
	}

	return FieldsMapping{Items: normalizeFieldsMappingItems(parsed)}, nil
}

func normalizeFieldsMappingItems(items map[string]string) map[string]string {
	if len(items) == 0 {
		return nil
	}

	normalized := make(map[string]string, len(items))
	for key, value := range items {
		normalized[strings.TrimSpace(key)] = strings.TrimSpace(value)
	}

	return normalized
}

func (m FieldsMapping) Projection(formatSource func(string) string, formatTarget func(string) string) (string, error) {
	if len(m.Items) > 0 {
		sources := make([]string, 0, len(m.Items))
		for source := range m.Items {
			sources = append(sources, source)
		}
		sort.Strings(sources)

		projection := make([]string, 0, len(sources))
		for _, source := range sources {
			source = strings.TrimSpace(source)
			target := strings.TrimSpace(m.Items[source])
			if source == "" || target == "" {
				return "", fmt.Errorf("fields_mapping contains empty key or value")
			}
			if formatSource != nil {
				source = formatSource(source)
			}
			if formatTarget != nil {
				target = formatTarget(target)
			}
			projection = append(projection, fmt.Sprintf("%s AS %s", source, target))
		}

		return strings.Join(projection, ", "), nil
	}

	return "*", nil
}

// TargetColumn 返回源字段在目标表中的列名。
// 配置了 fields_mapping 且能匹配到该源字段时返回映射后的目标列名；
// 否则（无映射或未匹配，如透传 "*" 场景）返回原字段名。
// 用于对目标/暂存表按列名做聚合（如 MAX(incr_field)），
// 避免源字段被改名后仍用源名去查目标表而报列不存在。
func (m FieldsMapping) TargetColumn(sourceField string) string {
	sourceField = strings.TrimSpace(sourceField)
	if target, ok := m.Items[sourceField]; ok {
		if trimmed := strings.TrimSpace(target); trimmed != "" {
			return trimmed
		}
	}
	return sourceField
}

func (m FieldsMapping) IsEmpty() bool {
	return len(m.Items) == 0
}

func (m FieldsMapping) Validate() error {
	if len(m.Items) == 0 {
		return nil
	}

	sources := make([]string, 0, len(m.Items))
	for source := range m.Items {
		sources = append(sources, source)
	}
	sort.Strings(sources)

	for _, source := range sources {
		target := strings.TrimSpace(m.Items[source])
		if target == "" {
			return fmt.Errorf("fields_mapping target field for source %q is empty", source)
		}
		if !isAllowedFieldName(target) {
			return fmt.Errorf("fields_mapping target field %q contains invalid characters; only letters, digits, and underscore are allowed", target)
		}
		if isReservedKeyword(target) {
			return fmt.Errorf("fields_mapping target field %q is a reserved keyword", target)
		}
		if !isAllowedFieldName(source) && !looksLikeExpression(source) {
			log.Printf("warning: fields_mapping source field %q contains special characters; only letters, digits, and underscore are recommended", source)
		}
	}

	return nil
}

// ============================================================================
// 字段名与保留字校验辅助
// ----------------------------------------------------------------------------
// 供 unpivot / fields_mapping 复用的标识符合法性、表达式识别与 SQL 保留字判断。
// ============================================================================

func isAllowedFieldName(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') || (char >= '0' && char <= '9') || char == '_' {
			continue
		}
		return false
	}
	return true
}

func looksLikeExpression(value string) bool {
	value = strings.TrimSpace(value)
	if value == "" {
		return false
	}
	for _, token := range []string{"(", ")", " ", "'", `"`, "+", "-", "*", "/", "%", "=", ","} {
		if strings.Contains(value, token) {
			return true
		}
	}
	return false
}

func isReservedKeyword(value string) bool {
	_, ok := reservedKeywords[strings.ToLower(strings.TrimSpace(value))]
	return ok
}

var reservedKeywords = map[string]struct{}{
	"add":        {},
	"all":        {},
	"alter":      {},
	"and":        {},
	"any":        {},
	"as":         {},
	"asc":        {},
	"by":         {},
	"case":       {},
	"check":      {},
	"column":     {},
	"constraint": {},
	"create":     {},
	"current":    {},
	"database":   {},
	"default":    {},
	"delete":     {},
	"desc":       {},
	"distinct":   {},
	"drop":       {},
	"else":       {},
	"exists":     {},
	"false":      {},
	"for":        {},
	"foreign":    {},
	"from":       {},
	"full":       {},
	"group":      {},
	"having":     {},
	"in":         {},
	"index":      {},
	"inner":      {},
	"insert":     {},
	"into":       {},
	"is":         {},
	"join":       {},
	"key":        {},
	"left":       {},
	"like":       {},
	"limit":      {},
	"not":        {},
	"null":       {},
	"offset":     {},
	"on":         {},
	"or":         {},
	"order":      {},
	"outer":      {},
	"primary":    {},
	"references": {},
	"right":      {},
	"select":     {},
	"set":        {},
	"table":      {},
	"then":       {},
	"top":        {},
	"true":       {},
	"union":      {},
	"unique":     {},
	"update":     {},
	"user":       {},
	"using":      {},
	"values":     {},
	"view":       {},
	"when":       {},
	"where":      {},
}
