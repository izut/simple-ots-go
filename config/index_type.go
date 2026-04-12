package config

import (
	"fmt"
	"strings"
)

// ParseIndexYAMLType 解析 tables.yaml 中 indexes[].indexType。
// 空字符串或 global 表示全局二级索引；local 表示本地二级索引。大小写不敏感。
func ParseIndexYAMLType(s string) (isLocal bool, err error) {
	t := strings.ToUpper(strings.TrimSpace(s))
	switch t {
	case "", "GLOBAL":
		return false, nil
	case "LOCAL":
		return true, nil
	default:
		return false, fmt.Errorf("indexType 仅支持 global 或 local，得到 %q", s)
	}
}

// FormatIndexYAMLType 将是否为本地索引写成 YAML 推荐取值（global / local）。
func FormatIndexYAMLType(isLocal bool) string {
	if isLocal {
		return "local"
	}
	return "global"
}

// PrimaryKeyTypeResult 表示主键类型解析结果，与 TableStore SDK 的 PrimaryKeyType 对应。
// 使用独立枚举而非直接依赖 SDK 类型，保持 config 包的零耦合。
type PrimaryKeyTypeResult int

const (
	PrimaryKeyTypeString  PrimaryKeyTypeResult = iota
	PrimaryKeyTypeInteger
	PrimaryKeyTypeBinary
)

// ParsePrimaryKeyType 解析 tables.yaml 中主键类型字符串。
// 支持 STRING / INTEGER（别名 INT）/ BINARY，大小写不敏感。
func ParsePrimaryKeyType(s string) (PrimaryKeyTypeResult, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "STRING":
		return PrimaryKeyTypeString, nil
	case "INTEGER", "INT":
		return PrimaryKeyTypeInteger, nil
	case "BINARY":
		return PrimaryKeyTypeBinary, nil
	default:
		return 0, fmt.Errorf("不支持的主键类型: %q", s)
	}
}

// ColumnTypeResult 表示属性列类型解析结果。
type ColumnTypeResult int

const (
	ColumnTypeString  ColumnTypeResult = iota
	ColumnTypeInteger
	ColumnTypeBinary
	ColumnTypeBoolean
	ColumnTypeDouble
)

// ParseColumnType 解析 tables.yaml 中属性列类型字符串。
// 支持 STRING / INTEGER（别名 INT）/ BINARY / BOOLEAN（别名 BOOL）/ DOUBLE（别名 FLOAT），大小写不敏感。
func ParseColumnType(s string) (ColumnTypeResult, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "STRING":
		return ColumnTypeString, nil
	case "INTEGER", "INT":
		return ColumnTypeInteger, nil
	case "BINARY":
		return ColumnTypeBinary, nil
	case "BOOLEAN", "BOOL":
		return ColumnTypeBoolean, nil
	case "DOUBLE", "FLOAT":
		return ColumnTypeDouble, nil
	default:
		return 0, fmt.Errorf("不支持的属性列类型: %q", s)
	}
}

// DefinedColumnTypeResult 表示预定义列类型解析结果，与 TableStore SDK 的 DefinedColumnType 对应。
type DefinedColumnTypeResult int

const (
	DefinedColumnString  DefinedColumnTypeResult = iota
	DefinedColumnInteger
	DefinedColumnBinary
	DefinedColumnBoolean
	DefinedColumnDouble
)

// ParseDefinedColumnType 解析 tables.yaml 中预定义列类型字符串。
// 与 ParseColumnType 相比，额外支持 BLOB 作为 BINARY 的别名，与 sync_tables 工具保持一致。
func ParseDefinedColumnType(s string) (DefinedColumnTypeResult, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "STRING":
		return DefinedColumnString, nil
	case "INTEGER", "INT":
		return DefinedColumnInteger, nil
	case "BINARY", "BLOB":
		return DefinedColumnBinary, nil
	case "BOOLEAN", "BOOL":
		return DefinedColumnBoolean, nil
	case "DOUBLE", "FLOAT":
		return DefinedColumnDouble, nil
	default:
		return 0, fmt.Errorf("不支持的预定义列类型: %q", s)
	}
}

// PrimaryKeyTypeToString 将 PrimaryKeyTypeResult 转为 YAML 推荐的大写字符串。
func PrimaryKeyTypeToString(t PrimaryKeyTypeResult) string {
	switch t {
	case PrimaryKeyTypeInteger:
		return "INTEGER"
	case PrimaryKeyTypeBinary:
		return "BINARY"
	default:
		return "STRING"
	}
}

// ColumnTypeToString 将 ColumnTypeResult 转为 YAML 推荐的大写字符串。
func ColumnTypeToString(t ColumnTypeResult) string {
	switch t {
	case ColumnTypeInteger:
		return "INTEGER"
	case ColumnTypeBinary:
		return "BINARY"
	case ColumnTypeBoolean:
		return "BOOLEAN"
	case ColumnTypeDouble:
		return "DOUBLE"
	default:
		return "STRING"
	}
}

// DefinedColumnTypeToString 将 DefinedColumnTypeResult 转为 YAML 推荐的大写字符串。
func DefinedColumnTypeToString(t DefinedColumnTypeResult) string {
	switch t {
	case DefinedColumnInteger:
		return "INTEGER"
	case DefinedColumnBinary:
		return "BINARY"
	case DefinedColumnBoolean:
		return "BOOLEAN"
	case DefinedColumnDouble:
		return "DOUBLE"
	default:
		return "STRING"
	}
}
