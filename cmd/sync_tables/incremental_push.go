// incremental_push 实现推送模式下「表已存在」时的增量 DDL：
// 在不动主键、不删列的前提下，用 AddDefinedColumn 补齐本地 YAML 中多出的预定义列。
package main

import (
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	"github.com/izut/simple-ots-go/config"
)

// assertPrimaryKeysMatchRemote 校验远程 DescribeTable 得到的主键与本地 YAML 是否完全一致（列数、名称顺序、类型）。
// 任一项不一致则返回错误：OTS 不支持在线改主键，需由用户自行评估是否 -force 删表重建。
func assertPrimaryKeysMatchRemote(desc *tablestore.DescribeTableResponse, tc config.TableConfig) error {
	if desc == nil || desc.TableMeta == nil {
		return fmt.Errorf("DescribeTable 返回的表元信息为空")
	}
	remote := desc.TableMeta.SchemaEntry
	if len(remote) != len(tc.PrimaryKeys) {
		return fmt.Errorf("主键列数不一致（远程 %d，本地 %d），修改主键需使用 -force 删表重建", len(remote), len(tc.PrimaryKeys))
	}
	for i := range remote {
		rpk := remote[i]
		lpk := tc.PrimaryKeys[i]
		if rpk == nil || rpk.Name == nil || rpk.Type == nil {
			return fmt.Errorf("远程主键 schema 第 %d 项数据不完整", i)
		}
		rname := strings.TrimSpace(*rpk.Name)
		lname := strings.TrimSpace(lpk.Name)
		if rname != lname {
			return fmt.Errorf("主键第 %d 列名称不一致（远程 %q，本地 %q）", i+1, rname, lname)
		}
		remoteTypeStr := primaryKeyTypeToString(*rpk.Type)
		lt := strings.ToUpper(strings.TrimSpace(lpk.Type))
		if lt == "INT" {
			lt = "INTEGER"
		}
		if lt != remoteTypeStr {
			return fmt.Errorf("主键列 %s 类型不一致（远程 %s，本地 %s）", lname, remoteTypeStr, lpk.Type)
		}
	}
	return nil
}

// diffDefinedColumnsToAdd 计算「本地比远程多」的预定义列集合（纯增量，不向远程删列）。
// 若某列在远程已存在但类型与本地 YAML 声明不一致，返回错误以免误调用 API。
func diffDefinedColumnsToAdd(remote []*tablestore.DefinedColumnSchema, local []config.DefinedColumn) ([]config.DefinedColumn, error) {
	if remote == nil {
		remote = []*tablestore.DefinedColumnSchema{}
	}
	remoteByName := make(map[string]tablestore.DefinedColumnType, len(remote))
	for _, c := range remote {
		if c == nil {
			continue
		}
		remoteByName[c.Name] = c.ColumnType
	}
	var toAdd []config.DefinedColumn
	for _, lc := range local {
		name := strings.TrimSpace(lc.Name)
		if name == "" {
			continue
		}
		rt, ok := remoteByName[name]
		if !ok {
			toAdd = append(toAdd, lc)
			continue
		}
		lt, err := yamlDefinedColumnType(lc.Type)
		if err != nil {
			return nil, fmt.Errorf("属性列 %s: %w", name, err)
		}
		if rt != lt {
			return nil, fmt.Errorf("预定义列 %q 远程类型为 %s，本地为 %s，类型冲突无法自动对齐", name, definedColumnTypeToString(rt), lc.Type)
		}
	}
	return toAdd, nil
}

// applyAddDefinedColumns 调用 TableStore AddDefinedColumn，单次请求附带多列以降低 RPC 次数。
func applyAddDefinedColumns(client *tablestore.TableStoreClient, tableName string, cols []config.DefinedColumn) error {
	if len(cols) == 0 {
		return nil
	}
	req := &tablestore.AddDefinedColumnRequest{TableName: tableName}
	for _, col := range cols {
		dt, err := yamlDefinedColumnType(col.Type)
		if err != nil {
			return fmt.Errorf("属性列 %s: %w", col.Name, err)
		}
		req.AddDefinedColumn(col.Name, dt)
	}
	_, err := client.AddDefinedColumn(req)
	return err
}
