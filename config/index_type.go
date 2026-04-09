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
