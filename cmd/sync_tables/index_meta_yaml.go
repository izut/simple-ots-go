// index_meta_yaml 将 config.Index 中的 indexType 映射到 TableStore SDK 的 SetAsGlobalIndex / SetAsLocalIndex。
package main

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	"github.com/izut/simple-ots-go/config"
)

// applyIndexMetaFromYAML 根据 tables.yaml 中 indexes[].indexType 设置 SDK 索引类型（全局 / 本地二级索引）。
func applyIndexMetaFromYAML(im *tablestore.IndexMeta, indexTypeYAML string) error {
	local, err := config.ParseIndexYAMLType(indexTypeYAML)
	if err != nil {
		return err
	}
	if local {
		im.SetAsLocalIndex()
	} else {
		im.SetAsGlobalIndex()
	}
	return nil
}

// indexTypeLabel 返回用于日志的中文描述（全局 / 本地）；解析失败时默认「全局」。
func indexTypeLabel(indexTypeYAML string) string {
	local, err := config.ParseIndexYAMLType(indexTypeYAML)
	if err != nil {
		return "全局"
	}
	if local {
		return "本地"
	}
	return "全局"
}
