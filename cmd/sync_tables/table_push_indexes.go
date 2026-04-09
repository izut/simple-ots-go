// table_push_indexes：按「表名」推送时，若本地 indexes 在远程不存在则 CreateIndex 补齐。
package main

import (
	"fmt"
	"log"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	"github.com/izut/simple-ots-go/config"
)

// remoteIndexNameSet 根据 DescribeTable 结果构造远程已有索引名集合。
func remoteIndexNameSet(desc *tablestore.DescribeTableResponse) map[string]struct{} {
	out := make(map[string]struct{})
	if desc == nil {
		return out
	}
	for _, idx := range desc.IndexMetas {
		out[idx.IndexName] = struct{}{}
	}
	return out
}

// ensureLocalIndexesOnRemote 在远程主表已存在且与本地主键一致的前提下，为 YAML 中列出但远程没有的索引调用 CreateIndex。
func ensureLocalIndexesOnRemote(client *tablestore.TableStoreClient, tc config.TableConfig, desc *tablestore.DescribeTableResponse) error {
	remote := remoteIndexNameSet(desc)
	for _, idx := range tc.Indexes {
		if _, ok := remote[idx.Name]; ok {
			continue
		}
		log.Printf("[sync_tables]     远程无索引 %q，CreateIndex（%s）...", idx.Name, indexTypeLabel(idx.IndexType))
		if err := syncOneIndex(client, tc, idx); err != nil {
			return fmt.Errorf("CreateIndex(%s): %w", idx.Name, err)
		}
		log.Printf("[sync_tables]     索引已创建: %s", idx.Name)
	}
	return nil
}

// dryRunMissingIndexes 演练：打印将对缺失索引执行的 CreateIndex 说明（不写远程）。
func dryRunMissingIndexes(tc config.TableConfig, desc *tablestore.DescribeTableResponse) {
	remote := remoteIndexNameSet(desc)
	for _, idx := range tc.Indexes {
		if _, ok := remote[idx.Name]; ok {
			continue
		}
		log.Printf("[sync_tables]   [dry-run] 远程无索引 %q，将 CreateIndex（%s）", idx.Name, indexTypeLabel(idx.IndexType))
	}
}
