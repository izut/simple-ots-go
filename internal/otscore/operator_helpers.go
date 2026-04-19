package otscore

import (
	"math"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
)

// normalizeIntegerToInt64 将非 int64 的 Go 整型（含 uint 族）转为 int64，便于 OTS INTEGER 属性列与 SDK AddColumn 接受；已是 int64 或非整型则原样返回。
// uint64 若大于 math.MaxInt64，为避免静默截断，保持原值返回（由服务端或调用方处理）。
func normalizeIntegerToInt64(v interface{}) interface{} {
	switch n := v.(type) {
	case int:
		return int64(n)
	case int8:
		return int64(n)
	case int16:
		return int64(n)
	case int32:
		return int64(n)
	case int64:
		return n
	case uint:
		return int64(n)
	case uint8:
		return int64(n)
	case uint16:
		return int64(n)
	case uint32:
		return int64(n)
	case uint64:
		if n > math.MaxInt64 {
			return v
		}
		return int64(n)
	default:
		return v
	}
}

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

// defaultPutRowCondition 若调用方未传 cond，则与历史行为一致：不校验行是否存在（覆盖写入）。
func defaultPutRowCondition(cond *tablestore.RowCondition) *tablestore.RowCondition {
	if cond != nil {
		return cond
	}
	return &tablestore.RowCondition{
		RowExistenceExpectation: tablestore.RowExistenceExpectation_IGNORE,
	}
}

// defaultUpdateRowCondition 若调用方未传 cond，则与单行 UpdateRow(cond==nil) 一致：要求行已存在（EXPECT_EXIST）。
func defaultUpdateRowCondition(cond *tablestore.RowCondition) *tablestore.RowCondition {
	if cond != nil {
		return cond
	}
	return &tablestore.RowCondition{
		RowExistenceExpectation: tablestore.RowExistenceExpectation_EXPECT_EXIST,
	}
}

// defaultDeleteRowCondition 若调用方未传 cond，则与历史行为一致：校验行是否存在（删除写入）。
func defaultDeleteRowCondition(cond *tablestore.RowCondition) *tablestore.RowCondition {
	if cond != nil {
		return cond
	}
	return &tablestore.RowCondition{
		RowExistenceExpectation: tablestore.RowExistenceExpectation_EXPECT_EXIST,
	}
}

// assemblePutRowChangeFromMap 根据一行数据构建 PutRowChange：按 tables.yaml 解析主键、*_json 编码、剥离主键列后写入属性列。
// cond 为 nil 时使用 RowExistenceExpectation_IGNORE（与旧版 PutRow 默认一致）。
func (op *SimpleTableOperator) assemblePutRowChangeFromMap(row map[string]interface{}, cond *tablestore.RowCondition) (*tablestore.PutRowChange, error) {
	return op.assemblePutRowChangeFromMapWithPKNames(row, cond, nil)
}

// assemblePutRowChangeFromMapWithPKNames 与 assemblePutRowChangeFromMap 相同，但可传入已构建的主键列名集合。
// 批量写入时对每一行复用同一 pkNames，避免重复分配 map、降低 GC 压力。
// pkNames 为 nil 时在函数内调用 primaryKeyNameSet()，行为与 assemblePutRowChangeFromMap 完全一致。
func (op *SimpleTableOperator) assemblePutRowChangeFromMapWithPKNames(row map[string]interface{}, cond *tablestore.RowCondition, pkNames map[string]bool) (*tablestore.PutRowChange, error) {
	if pkNames == nil {
		pkNames = op.primaryKeyNameSet()
	}
	pk, err := op.buildPrimaryKey(row)
	if err != nil {
		return nil, err
	}
	// 浅拷贝并对 *_json 列做 JSON 序列化（结构化 → 字符串），不修改调用方 data
	rowOTS := shallowCopyMap(row)
	if err := encodeJSONSuffixColumnsInMap(rowOTS); err != nil {
		return nil, err
	}
	ch := &tablestore.PutRowChange{
		TableName:  op.tableName,
		PrimaryKey: pk,
		Condition:  defaultPutRowCondition(cond),
	}
	for colName, val := range rowOTS {
		if pkNames[colName] {
			continue
		}
		// 属性列中常见 int 字面量等非 int64 整型统一为 int64，避免 SDK/OTS 对 INTEGER 列类型不匹配。
		ch.AddColumn(colName, normalizeIntegerToInt64(val))
	}
	return ch, nil
}
