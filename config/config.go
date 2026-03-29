package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	TaskName    string       `yaml:"task_name"`
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
)

type TaskConfig struct {
	Type TaskType `yaml:"type"`

	Sources []SourceConfig `yaml:"sources"`

	Downstream *DownstreamConfig `yaml:"downstream"`
}

type TaskType string

const (
	TaskTypeSQL   TaskType = "query"
	TaskTypeTable TaskType = "exec"
)

type SourceConfig struct {
	DB    string `yaml:"db"`
	SQL   string `yaml:"sql"`
	Table string `yaml:"table"`
}

type DownstreamConfig struct {
	DB    string   `yaml:"db"`
	Table string   `yaml:"table"`
	Mode  ModeType `yaml:"mode"`
}

type ModeType string

const (
	ModeTypeCopy   ModeType = "copy"
	ModeTypeFull   ModeType = "full"
	ModeTypeAppend ModeType = "append"
	ModeTypeUpsert ModeType = "upsert"
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
		for _, v := range t.Sources {
			if v.SQL == "" && v.Table == "" {
				return fmt.Errorf("task SQL or table required")
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

	case DBTypePG:

		return fmt.Sprintf(
			"postgresql://%s:%s@%s:%d/%s",
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
