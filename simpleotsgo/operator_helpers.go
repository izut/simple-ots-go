package simpleotsgo

import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
)

// tsClient 返回与当前操作器共底座的 *Client，复用 row_api 上的封装（重试、JSON 列等）。
func (op *SimpleTableOperator) tsClient() *Client {
	return &Client{client: op.client}
}

// primaryKeyNameSet 当前表主键列名集合，用于从整行 map 中筛出属性列。
func (op *SimpleTableOperator) primaryKeyNameSet() map[string]bool {
	s := make(map[string]bool, len(op.config.PrimaryKey))
	for _, c := range op.config.PrimaryKey {
		s[c.ColumnName] = true
	}
	return s
}

// assemblePutRowChangeFromMap 根据一行数据构建 PutRowChange：按 tables.yaml 解析主键、*_json 编码、剥离主键列后写入属性列。
func (op *SimpleTableOperator) assemblePutRowChangeFromMap(row map[string]interface{}) (*tablestore.PutRowChange, error) {
	// 构建主键
	pk, err := op.buildPrimaryKey(row)
	if err != nil {
		return nil, err
	}
	// 浅拷贝并对 *_json 列做 JSON 序列化（结构化 → 字符串），不修改调用方 data
	rowOTS := shallowCopyMap(row)
	if err := encodeJSONSuffixColumnsInMap(rowOTS); err != nil {
		return nil, err
	}
	pkNames := op.primaryKeyNameSet()
	ch := &tablestore.PutRowChange{
		TableName:  op.tableName,
		PrimaryKey: pk,
		Condition: &tablestore.RowCondition{
			RowExistenceExpectation: tablestore.RowExistenceExpectation_IGNORE,
		},
	}
	for colName, val := range rowOTS {
		if pkNames[colName] {
			continue
		}
		ch.AddColumn(colName, val)
	}
	return ch, nil
}
