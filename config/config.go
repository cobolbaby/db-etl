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
	// MetaDB 指定存放 manager.job_data_sync_v2 配置表的数据库别名（引用 databases[].name）。
	// 当通过 -job 参数从数据库加载任务列表时必填。
	MetaDB string `yaml:"meta_db"`
}

type DBConfig struct {
	Name     string `yaml:"name"`
	Type     DBType `yaml:"type"` // mssql / pg
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
	// QueryTimeout 单条查询语句的超时（秒），0 表示不限制（默认）。
	// 仅影响读取端 SELECT 查询; 写入端的 COPY 不受此限制。
	QueryTimeout int `yaml:"query_timeout"`
	// StatementTimeout 写入端单条语句的执行超时（秒），0 表示不限制（默认）。
	// 通过 PostgreSQL 会话参数 statement_timeout 实现; merge 模式下 DELETE+INSERT 耗时较长，建议设为 0（不限制）或足够大的值。
	StatementTimeout int `yaml:"statement_timeout"`
}

type DBType string

const (
	DBTypeMSSQL DBType = "mssql"
	DBTypePG    DBType = "postgres"
	DBTypeGP    DBType = "greenplum"
)

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
	DBName         string        `yaml:"dbname"`
	SQL            string        `yaml:"sql"`
	Table          string        `yaml:"table"`           // SQL 和 Table 至少要指定一个，SQL 优先级更高
	WhereStatement string        `yaml:"where_statement"` // Table 场景下的附加过滤条件；SQL 场景下会作为外层过滤条件追加
	FieldsMapping  FieldsMapping `yaml:"fields_mapping"`  // Table 场景下的字段映射，格式为 map[源字段/表达式]目标字段
	BatchSize      int           `yaml:"batch_size"`
	DBType         DBType        `yaml:"-"`
	Mode           ModeType      `yaml:"-"`
	IncrField      string        `yaml:"incr_field"` // 用于增量抽取，指定一个日期/时间字段，配合 Watermark 实现增量抽取
	IncrPoint      string        `yaml:"incr_point"` // 增量抽取的起点
	OrderBy        string        `yaml:"order_by"`   // OrderBy 指定查询排序字段。当 target.commit_batch_size > 0 时必须有序，框架会自动设为 src_incr_field，也可手动指定其他表达式（如 "id ASC"）。
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

func (m FieldsMapping) IsEmpty() bool {
	return len(m.Items) == 0
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
		if !looksLikeExpression(source) && !isAllowedFieldName(source) {
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
	DBName    string   `yaml:"dbname"`
	Table     string   `yaml:"table"`
	Mode      ModeType `yaml:"mode"`
	IncrField string
	PK        string `yaml:"pk"`
	// CommitBatchSize 控制 merge 模式下每隔多少个 batch 提交一次事务并更新水位。
	// 0 表示不分段，整个任务在单个事务中完成（原有行为）。
	// 适用于超大表，设置后可在中断重启后从上次水位断点续传。
	CommitBatchSize int `yaml:"commit_batch_size"`
}

type ModeType string

const (
	ModeTypeCopy   ModeType = "copy"
	ModeTypeFull   ModeType = "full"
	ModeTypeAppend ModeType = "append"
	ModeTypeMerge  ModeType = "merge"
	ModeTypeCDC    ModeType = "cdc"
)

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

	dbTypes, err := c.validateDatabases()
	if err != nil {
		return err
	}

	if err := c.validateTasks(dbTypes); err != nil {
		return err
	}

	return nil
}

func (c *Config) validateDatabases() (map[string]DBType, error) {
	dbTypes := make(map[string]DBType, len(c.Databases))
	for _, db := range c.Databases {
		if db.Name == "" {
			return nil, fmt.Errorf("database name required")
		}
		if db.Type == "" {
			return nil, fmt.Errorf("database type required for %s", db.Name)
		}
		dbTypes[db.Name] = db.Type
	}
	return dbTypes, nil
}

func (c *Config) validateTasks(dbTypes map[string]DBType) error {

	if len(c.Tasks) == 0 && c.MetaDB == "" {
		return fmt.Errorf("tasks cannot be empty (set 'meta_db' to load tasks from database)")
	}

	for _, t := range c.Tasks {
		if err := validateTask(t, dbTypes); err != nil {
			return err
		}
	}

	return nil
}

func validateTask(task TaskConfig, dbTypes map[string]DBType) error {
	if task.Target == nil {
		return fmt.Errorf("target must be specified")
	}
	if err := validateTarget(task.Target, dbTypes); err != nil {
		return err
	}

	for _, source := range task.Sources {
		if err := validateSource(source, task.Target, dbTypes); err != nil {
			return err
		}
	}

	return nil
}

func validateTarget(target *TargetConfig, dbTypes map[string]DBType) error {
	if target.DBName == "" {
		return fmt.Errorf("target dbname is required")
	}
	if _, ok := dbTypes[target.DBName]; !ok {
		return fmt.Errorf("target db not found: %s", target.DBName)
	}
	if strings.TrimSpace(target.Table) == "" {
		return fmt.Errorf("target table is required")
	}
	return nil
}

func validateSource(source *SourceConfig, target *TargetConfig, dbTypes map[string]DBType) error {
	if source == nil {
		return fmt.Errorf("source must be specified")
	}
	if source.DBName == "" {
		return fmt.Errorf("source dbname is required")
	}

	dbType, ok := dbTypes[source.DBName]
	if !ok {
		return fmt.Errorf("source db not found: %s", source.DBName)
	}

	source.normalize()
	source.DBType = dbType

	if err := source.FieldsMapping.Validate(); err != nil {
		return err
	}

	if source.BatchSize <= 0 {
		source.BatchSize = 10000
	}

	if source.SQL == "" && source.Table == "" {
		return fmt.Errorf("sql or table must be specified")
	}

	if source.SQL != "" && source.Table != "" {
		return fmt.Errorf("sql and table cannot both be specified")
	}

	if err := ValidateSourceTableName(source.Table, source.DBType); err != nil {
		return err
	}

	if target.CommitBatchSize > 0 {
		if source.IncrField == "" {
			return fmt.Errorf("incr_field is required when commit_batch_size > 0")
		}
		if source.OrderBy == "" {
			source.OrderBy = source.IncrField + " ASC"
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
返回数据库连接字符串
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
		return fmt.Sprintf(
			"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable",
			db.Host,
			db.Port,
			db.User,
			db.Password,
			db.Database,
		)

	default:
		panic("unsupported db type: " + db.Type)

	}
}
