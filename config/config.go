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
}

type DBConfig struct {
	Name     string `yaml:"name"`
	Type     DBType `yaml:"type"` // mssql / pg
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
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
	DBName string `yaml:"dbname"`
	SQL    string `yaml:"sql"`
	Table     string `yaml:"table"` // SQL 和 Table 至少要指定一个，SQL 优先级更高
	BatchSize int    `yaml:"batch_size"`
	Mode      ModeType
	IncrField string `yaml:"src_incr_field"` // 用于增量抽取，指定一个日期/时间字段，配合 Watermark 实现增量抽取
	IncrPoint string `yaml:"incr_point"`     // 增量抽取的起点, 可以是一个具体的日期/时间值，也可以是一个占位符，如 ${WATERMARK}，表示从上次抽取的 Watermark 位置开始抽取
}

type TargetConfig struct {
	DBName    string   `yaml:"dbname"`
	Table     string   `yaml:"table"`
	Mode      ModeType `yaml:"mode"`
	IncrField string
	PK        string `yaml:"dst_pk"`
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

	if len(c.Tasks) == 0 {
		return fmt.Errorf("tasks cannot be empty")
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

			// if strings.TrimSpace(s.IncrField) != "" {
			// 	// ...
			// }
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

func splitQualifiedName(name string) (string, string, error) {
	trimmed := strings.TrimSpace(name)
	if trimmed == "" {
		return "", "", fmt.Errorf("qualified name is required")
	}

	parts := strings.SplitN(trimmed, ".", 2)
	if len(parts) != 2 {
		return "", "", fmt.Errorf("qualified name must be schema.table")
	}

	schema := strings.TrimSpace(parts[0])
	table := strings.TrimSpace(parts[1])
	if schema == "" || table == "" {
		return "", "", fmt.Errorf("qualified name must be schema.table")
	}

	return schema, table, nil
}
