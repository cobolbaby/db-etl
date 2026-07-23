package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ErrorPolicy string       `yaml:"error_policy"`
	Databases   []DBConfig   `yaml:"databases"`
	Tasks       []TaskConfig `yaml:"tasks"`
	Name        string       `yaml:"name"`
	Comment     string       `yaml:"comment"`
	// MetaDB 指定存放 manager.job_data_sync 配置表的数据库别名（引用 databases[].name）。
	// 当通过 -job 参数从数据库加载任务列表时必填。
	MetaDB string `yaml:"meta_db"`
	// Retry 配置任务失败时的重试策略。
	Retry *RetryPolicy `yaml:"retry"`
}

// RetryPolicy configures retry behavior for source-level failures.
type RetryPolicy struct {
	MaxAttempts     int `yaml:"max_attempts"`      // Max attempts including the first. Default 3.
	DelaySeconds    int `yaml:"delay_seconds"`     // Initial delay in seconds. Default 5.
	MaxDelaySeconds int `yaml:"max_delay_seconds"` // Max backoff delay in seconds. Default 60.
}

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
	// TimeZone 固定写入端 PostgreSQL 会话时区（如 "America/Mexico_City"、"UTC"、"+08"）。
	// 源端 SQL Server DATETIME/DATETIME2 无时区，序列化为无时区字符串后写入 timestamptz 列时，
	// PostgreSQL 会按会话时区解析。若不固定，会话时区将随运行环境（PGTZ/TZ/服务端默认）漂移，
	// 导致同一份数据在不同客户端时区下入库为不同的绝对时刻。设置本项以获得确定性行为。
	// 为空时不注入，保持服务端默认。
	TimeZone string `yaml:"timezone"`
}

type DBType string

const (
	DBTypeMSSQL DBType = "mssql"
	DBTypePG    DBType = "postgres"
	DBTypeGP    DBType = "greenplum"
)

var supportedDBTypes = map[DBType]struct{}{
	DBTypeMSSQL: {},
	DBTypePG:    {},
	DBTypeGP:    {},
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

// dbTypeAliases 将常见的数据库类型写法归一化为标准值。
var dbTypeAliases = map[string]DBType{
	"mssql":      DBTypeMSSQL,
	"sqlserver":  DBTypeMSSQL,
	"postgres":   DBTypePG,
	"postgresql": DBTypePG,
	"pg":         DBTypePG,
	"greenplum":  DBTypeGP,
	"gp":         DBTypeGP,
}

type TaskConfig struct {
	Name    string          `yaml:"name"`
	Type    TaskType        `yaml:"type"`
	Sources []*SourceConfig `yaml:"sources"`
	Target  *TargetConfig   `yaml:"target"`
}

type TaskType string

const (
	TaskTypeEtl  TaskType = "query"
	TaskTypeExec TaskType = "exec"
)

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
	// Transforms 定义字段级后处理转换规则（在类型序列化之后执行）。
	// Transforms []FieldTransformDef `yaml:"transforms"`
}

// FieldTransformDef defines a single field transformation rule in config.
// type FieldTransformDef struct {
// 	Column    string `yaml:"column"`
// 	Transform string `yaml:"transform"` // trim, upper, lower, replace, default, prefix, suffix
// 	Arg1      string `yaml:"arg1"`
// 	Arg2      string `yaml:"arg2"`
// }

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

func (s *SourceConfig) normalize() {
	s.WhereStatement = strings.TrimSpace(s.WhereStatement)
}

type TargetConfig struct {
	ConnID    string   `yaml:"conn_id"`   // 优先按 conn_id 匹配数据源，为空时回退到 conn_name
	ConnName  string   `yaml:"conn_name"` // 引用 databases[].name
	Table     string   `yaml:"table"`
	Mode      ModeType `yaml:"mode"`
	IncrField string
	PK        string `yaml:"pk"`
	// CommitBatchSize 控制 merge 模式下每隔多少个 batch 提交一次事务并更新水位。
	// 0 表示不分段，整个任务在单个事务中完成（原有行为）。
	// 适用于超大表，设置后可在中断重启后从上次水位断点续传。
	CommitBatchSize int `yaml:"commit_batch_size"`
	// TruncateTimeout full 模式下 TRUNCATE 尝试获取锁的超时时间（秒）。
	// 超时后自动退避为 DELETE FROM，以避免长时间阻塞下游。
	// 0 表示使用默认值（defaultTruncateTimeoutSec 秒）。
	TruncateTimeout int `yaml:"truncate_timeout"`
}

// defaultTruncateTimeoutSec 是 full 模式下 TRUNCATE 等锁超时的默认值（秒）。
// TargetConfig.TruncateTimeout==0 时采用此默认。
const defaultTruncateTimeoutSec = 10

// EffectiveTruncateTimeout 返回 TRUNCATE 等锁超时的有效秒数：
// 0 时使用内置默认值 defaultTruncateTimeoutSec，其余（含负值）原样返回。
// 负值语义为“不设超时”，由调用方判断。
func (t *TargetConfig) EffectiveTruncateTimeout() int {
	if t.TruncateTimeout == 0 {
		return defaultTruncateTimeoutSec
	}
	return t.TruncateTimeout
}

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
		c.ErrorPolicy = "abort"
	} else if c.ErrorPolicy != "abort" && c.ErrorPolicy != "continue" {
		return fmt.Errorf("invalid error_policy: %s", c.ErrorPolicy)
	}

	resolver, err := c.validateDatabases()
	if err != nil {
		return err
	}

	if err := c.validateTasks(resolver); err != nil {
		return err
	}

	return nil
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

func (c *Config) validateTasks(resolver DBResolver) error {

	if len(c.Tasks) == 0 && c.MetaDB == "" {
		return fmt.Errorf("tasks cannot be empty (set 'meta_db' to load tasks from database)")
	}

	for _, t := range c.Tasks {
		if err := validateTask(t, resolver); err != nil {
			return err
		}
	}

	return nil
}

func validateTask(task TaskConfig, resolver DBResolver) error {
	if task.Target == nil {
		return fmt.Errorf("target must be specified")
	}
	if err := validateTarget(task.Target, resolver); err != nil {
		return err
	}

	for _, source := range task.Sources {
		if err := validateSource(source, task.Target, resolver); err != nil {
			return err
		}
	}

	return nil
}

func validateTarget(target *TargetConfig, resolver DBResolver) error {
	if target.ConnID == "" && target.ConnName == "" {
		return fmt.Errorf("target conn_id or conn_name is required")
	}
	if _, ok := resolver.Resolve(target.ConnID, target.ConnName); !ok {
		return fmt.Errorf("target db not found (conn_id=%q conn_name=%q)", target.ConnID, target.ConnName)
	}
	if strings.TrimSpace(target.Table) == "" {
		return fmt.Errorf("target table is required")
	}
	if _, ok := supportedTargetModes[target.Mode]; !ok {
		return fmt.Errorf("unsupported target mode: %s", target.Mode)
	}
	return nil
}

func validateSource(source *SourceConfig, target *TargetConfig, resolver DBResolver) error {
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

	if err := ValidateSourceTableName(source.Table, source.DBType); err != nil {
		return err
	}

	if err := source.FieldsMapping.Validate(); err != nil {
		return err
	}

	if source.BatchSize <= 0 {
		source.BatchSize = 10000
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

func ValidateSourceTableName(table string, dbType DBType) error {
	trimmed := strings.TrimSpace(table)
	if trimmed == "" {
		return nil
	}

	parts := strings.Split(trimmed, ".")
	switch len(parts) {
	case 1, 2:
		return nil
	case 3:
		if dbType != DBTypeMSSQL {
			return fmt.Errorf("source table %q uses db.schema.table, which is only supported for mssql sources", trimmed)
		}
		return nil
	default:
		return fmt.Errorf("source table must be table, schema.table, or db.schema.table")
	}
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
		return fmt.Sprintf(
			"server=%s;user id=%s;password=%s;port=%d;database=%s",
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
		if len(opts) == 0 {
			return base
		}
		// libpq options 值含空格，须用单引号包裹；pgx 按 libpq 规则解析。
		return base + fmt.Sprintf(" options='%s'", strings.Join(opts, " "))

	default:
		panic("unsupported db type: " + db.Type)

	}
}
