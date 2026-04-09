package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v2"
)

// TableConfig 描述单张表的配置定义。
// 该结构与 tables.yaml 中每个 tables 元素一一对应。
type TableConfig struct {
	Name           string          `yaml:"name"`
	InstanceName   string          `yaml:"instanceName"`
	PrimaryKeys    []PrimaryKey    `yaml:"primaryKeys"`
	DefinedColumns []DefinedColumn `yaml:"definedColumns"`
	Indexes        []Index         `yaml:"indexes,omitempty"`
}

// PrimaryKey 主键配置
type PrimaryKey struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// DefinedColumn 定义的列配置
type DefinedColumn struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// Index 索引配置
type Index struct {
	Name            string          `yaml:"name"`
	IndexType       string          `yaml:"indexType,omitempty"` // 二级索引范围：global（默认）或 local
	PrimaryKeys     []PrimaryKey    `yaml:"primaryKeys"`
	DefinedColumns  []DefinedColumn `yaml:"definedColumns"`
	IncludeBaseData bool            `yaml:"includeBaseData"`
}

// TablesConfig 表配置集合
type TablesConfig struct {
	Tables []TableConfig `yaml:"tables"`
}

// LoadTablesConfig 从指定文件路径加载表配置。
// 当路径不存在、YAML 语法错误或字段不合法时，会返回带上下文的错误信息。
func LoadTablesConfig(filePath string) (*TablesConfig, error) {
	// 读取 YAML 文件原始内容。
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read tables.yaml: %w", err)
	}

	// 反序列化为结构体，供上层转换与校验。
	var cfg TablesConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal tables.yaml: %w", err)
	}

	return &cfg, nil
}

// LoadTablesConfigFromDir 读取目录下的 tables.yaml。
// 该方法适用于调用方仅提供“配置目录”而非具体文件路径的场景。
func LoadTablesConfigFromDir(dir string) (*TablesConfig, error) {
	filePath := filepath.Join(dir, "tables.yaml")
	return LoadTablesConfig(filePath)
}

// GetTableConfig 根据表名获取表配置
func (tc *TablesConfig) GetTableConfig(tableName string) (*TableConfig, error) {
	for _, table := range tc.Tables {
		if table.Name == tableName {
			return &table, nil
		}
	}
	return nil, fmt.Errorf("table config not found: %s", tableName)
}

// GetAllTableConfigs 获取所有表配置
func (tc *TablesConfig) GetAllTableConfigs() map[string]*TableConfig {
	configs := make(map[string]*TableConfig)
	for i := range tc.Tables {
		configs[tc.Tables[i].Name] = &tc.Tables[i]
	}
	return configs
}
