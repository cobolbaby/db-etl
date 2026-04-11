package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	ErrorPolicy string       `yaml:"error_policy"`
	BatchSize   int          `yaml:"batch_size"`
	Databases   []DBConfig   `yaml:"databases"`
	Tasks       []TaskConfig `yaml:"tasks"`
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
	Name       string            `yaml:"name"`
	Type       TaskType          `yaml:"type"`
	Comment    string            `yaml:"comment"`
	Sources    []*SourceConfig   `yaml:"sources"`
	Downstream *DownstreamConfig `yaml:"downstream"`
}

type TaskType string

const (
	TaskTypeSQL   TaskType = "query"
	TaskTypeTable TaskType = "exec"
)

type SourceConfig struct {
	Name      string   `yaml:"name"`
	SQL       string   `yaml:"sql"`
	Table     string   `yaml:"table"` // SQL 和 Table 至少要指定一个，SQL 优先级更高
	Mode      ModeType `yaml:"mode"`
	IncrField string   `yaml:"src_incr_field"` // 用于增量抽取，指定一个日期/时间字段，配合 Watermark 实现增量抽取
	IncrPoint string   `yaml:"incr_point"`     // 增量抽取的起点, 可以是一个具体的日期/时间值，也可以是一个占位符，如 ${WATERMARK}，表示从上次抽取的 Watermark 位置开始抽取
}

type DownstreamConfig struct {
	Name  string   `yaml:"name"`
	Table string   `yaml:"table"`
	Mode  ModeType `yaml:"mode"`
	PK    string   `yaml:"dst_pk"`
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

	if cfg.BatchSize == 0 {
		cfg.BatchSize = 20000
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
		for _, s := range t.Sources {
			if s.SQL == "" && s.Table == "" {
				return fmt.Errorf("sql or table must be specified")
			}

			if s.SQL != "" && s.Table != "" {
				return fmt.Errorf("sql and table cannot both be specified")
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
			"postgresql://%s:%s@%s:%d/%s?sslmode=disable",
			db.User,
			db.Password,
			db.Host,
			db.Port,
			db.Database,
		)

	default:
		panic("unsupported db type: " + db.Type)

	}
}
