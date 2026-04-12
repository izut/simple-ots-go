package otscore

import (
	"fmt"
	"strings"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	"github.com/izut/simple-ots-go/config"
)

// ParsePrimaryKeyType 解析主键类型字符串并返回 TableStore SDK 的 PrimaryKeyType。
// 内部委托 config.ParsePrimaryKeyType 做校验，再映射为 SDK 枚举值。
func ParsePrimaryKeyType(s string) (tablestore.PrimaryKeyType, error) {
	r, err := config.ParsePrimaryKeyType(s)
	if err != nil {
		return 0, err
	}
	switch r {
	case config.PrimaryKeyTypeInteger:
		return tablestore.PrimaryKeyType_INTEGER, nil
	case config.PrimaryKeyTypeBinary:
		return tablestore.PrimaryKeyType_BINARY, nil
	default:
		return tablestore.PrimaryKeyType_STRING, nil
	}
}

// ParseColumnType 解析属性列类型字符串并返回 TableStore SDK 的 ColumnType。
// 内部委托 config.ParseColumnType 做校验，再映射为 SDK 枚举值。
func ParseColumnType(s string) (tablestore.ColumnType, error) {
	r, err := config.ParseColumnType(s)
	if err != nil {
		return 0, err
	}
	switch r {
	case config.ColumnTypeInteger:
		return tablestore.ColumnType_INTEGER, nil
	case config.ColumnTypeBinary:
		return tablestore.ColumnType_BINARY, nil
	case config.ColumnTypeBoolean:
		return tablestore.ColumnType_BOOLEAN, nil
	case config.ColumnTypeDouble:
		return tablestore.ColumnType_DOUBLE, nil
	default:
		return tablestore.ColumnType_STRING, nil
	}
}

// ConvertTablesConfig 将 config 包中的 YAML 结构转换为运行时表配置。
func ConvertTablesConfig(cfg *config.TablesConfig) (map[string]*TableConfig, error) {
	result := make(map[string]*TableConfig, len(cfg.Tables))
	for _, t := range cfg.Tables {
		pkCols := make([]tablestore.PrimaryKeyColumn, 0, len(t.PrimaryKeys))
		for _, pk := range t.PrimaryKeys {
			if _, err := config.ParsePrimaryKeyType(pk.Type); err != nil {
				return nil, fmt.Errorf("table %s primary key %s type invalid: %w", t.Name, pk.Name, err)
			}
			pkCols = append(pkCols, tablestore.PrimaryKeyColumn{
				ColumnName:       pk.Name,
				Value:            nil,
				PrimaryKeyOption: tablestore.NONE,
			})
		}

		attrCols := make([]tablestore.AttributeColumn, 0, len(t.DefinedColumns))
		for _, c := range t.DefinedColumns {
			if _, err := config.ParseColumnType(c.Type); err != nil {
				return nil, fmt.Errorf("table %s column %s type invalid: %w", t.Name, c.Name, err)
			}
			attrCols = append(attrCols, tablestore.AttributeColumn{
				ColumnName: c.Name,
				Value:      nil,
				Timestamp:  0,
			})
		}

		result[t.Name] = &TableConfig{
			TableName:      t.Name,
			InstanceName:   t.InstanceName,
			RegionId:       strings.TrimSpace(t.RegionId),
			PrimaryKey:     pkCols,
			DefinedColumns: attrCols,
		}

		for _, idx := range t.Indexes {
			if strings.TrimSpace(idx.Name) == "" {
				return nil, fmt.Errorf("table %s has index with empty name", t.Name)
			}
			if _, duplicated := result[idx.Name]; duplicated {
				return nil, fmt.Errorf("duplicated table/index name in tables.yaml: %s", idx.Name)
			}

			indexPKCols := make([]tablestore.PrimaryKeyColumn, 0, len(idx.PrimaryKeys))
			for _, pk := range idx.PrimaryKeys {
				if _, err := config.ParsePrimaryKeyType(pk.Type); err != nil {
					return nil, fmt.Errorf("index %s primary key %s type invalid: %w", idx.Name, pk.Name, err)
				}
				indexPKCols = append(indexPKCols, tablestore.PrimaryKeyColumn{
					ColumnName:       pk.Name,
					Value:            nil,
					PrimaryKeyOption: tablestore.NONE,
				})
			}

			result[idx.Name] = &TableConfig{
				TableName:      idx.Name,
				InstanceName:   t.InstanceName,
				RegionId:       strings.TrimSpace(t.RegionId),
				PrimaryKey:     indexPKCols,
				DefinedColumns: nil,
			}
		}
	}
	return result, nil
}
