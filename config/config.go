package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Source    DBConfig   `yaml:"source"`
	Target    DBConfig   `yaml:"target"`
	Tables    []TableMap `yaml:"tables"`
	BatchSize int        `yaml:"batch_size"`
	Parallel  int        `yaml:"parallel"`
}

type TableMap struct {
	Src string `yaml:"src"`
	Dst string `yaml:"dst"`
}

type DBConfig struct {
	Type     string `yaml:"type"` // mssql / pg
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
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

	if cfg.BatchSize == 0 {
		cfg.BatchSize = 20000
	}

	if cfg.Parallel == 0 {
		cfg.Parallel = 4
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

	if c.Source.Type == "" {
		return fmt.Errorf("source.type required")
	}

	if c.Target.Type == "" {
		return fmt.Errorf("target.type required")
	}

	if len(c.Tables) == 0 {
		return fmt.Errorf("tables cannot be empty")
	}

	for _, t := range c.Tables {
		if t.Src == "" || t.Dst == "" {
			return fmt.Errorf("invalid table mapping: %+v", t)
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

	case "mssql":

		return fmt.Sprintf(
			"server=%s;user id=%s;password=%s;port=%d;database=%s",
			db.Host,
			db.User,
			db.Password,
			db.Port,
			db.Database,
		)

	case "pg":

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
