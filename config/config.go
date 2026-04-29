package config

import (
	"fmt"
	"os"
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
	DBName    string `yaml:"dbname"`
	SQL       string `yaml:"sql"`
	Table     string `yaml:"table"` // SQL 和 Table 至少要指定一个，SQL 优先级更高
	BatchSize int    `yaml:"batch_size"`
	Mode      ModeType
	IncrField string `yaml:"incr_field"` // 用于增量抽取，指定一个日期/时间字段，配合 Watermark 实现增量抽取
	IncrPoint string `yaml:"incr_point"` // 增量抽取的起点
	OrderBy   string `yaml:"order_by"`   // OrderBy 指定查询排序字段。当 target.commit_batch_size > 0 时必须有序，框架会自动设为 src_incr_field，也可手动指定其他表达式（如 "id ASC"）。
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

	for _, db := range c.Databases {
		if db.Name == "" {
			return fmt.Errorf("database name required")
		}
		if db.Type == "" {
			return fmt.Errorf("database type required for %s", db.Name)
		}
	}

	if len(c.Tasks) == 0 && c.MetaDB == "" {
		return fmt.Errorf("tasks cannot be empty (set 'meta_db' to load tasks from database)")
	}

	for _, t := range c.Tasks {
		if t.Target == nil {
			return fmt.Errorf("target must be specified")
		}

		for _, s := range t.Sources {
			if s.BatchSize <= 0 {
				s.BatchSize = 10000
			}

			if s.SQL == "" && s.Table == "" {
				return fmt.Errorf("sql or table must be specified")
			}

			if s.SQL != "" && s.Table != "" {
				return fmt.Errorf("sql and table cannot both be specified")
			}

			// 分段提交时必须保证有序，自动补齐 OrderBy
			if t.Target.CommitBatchSize > 0 {
				if s.IncrField == "" {
					return fmt.Errorf("incr_field is required when commit_batch_size > 0")
				}
				if s.OrderBy == "" {
					s.OrderBy = s.IncrField + " ASC"
				}
			}
		}
	}

	return nil
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
