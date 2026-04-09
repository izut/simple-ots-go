package simpleotsgo

import (
	"encoding/json"
	"fmt"
	"strings"
)

// jsonColumnSuffix 约定：列名后缀匹配时，OTS 中以 STRING 存储 JSON 文本，SDK 读出时解析为 map/slice 等，写入时将结构化值序列化为字符串。
// TableStore 无原生 JSON 类型，项目内 tables.yaml 常用 xxx_json 命名存放 JSON 字符串。
const jsonColumnSuffix = "_json"

// shallowCopyMap 浅拷贝 map，避免写入路径修改调用方传入的 map。
func shallowCopyMap(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return nil
	}
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// cellToJSONSourceString 将单元格值转为可参与 json.Unmarshal 的源字符串（OTS 读出的属性列多为 string 或 []byte）。
func cellToJSONSourceString(v interface{}) (string, bool) {
	switch t := v.(type) {
	case string:
		return t, true
	case []byte:
		return string(t), true
	default:
		return "", false
	}
}

// decodeJSONSuffixColumns 就地解析：凡列名以 _json 结尾且值为字符串类，尝试 json.Unmarshal 为 interface{}（对象→map、数组→slice）；非法 JSON 或空串则保持原值。
func decodeJSONSuffixColumns(m map[string]interface{}) {
	if m == nil {
		return
	}
	for k, v := range m {
		if !strings.HasSuffix(k, jsonColumnSuffix) {
			continue
		}
		s, ok := cellToJSONSourceString(v)
		if !ok || strings.TrimSpace(s) == "" {
			continue
		}
		var parsed interface{}
		if err := json.Unmarshal([]byte(s), &parsed); err != nil {
			continue
		}
		m[k] = parsed
	}
}

// RowMapFromTableStore 将 OTS 读出并已装入 map 的一行数据做 *_json 列反序列化（拷贝新 map，不修改 raw）。
// 若自行通过 tablestore.TableStoreClient 读数，可用此函数与 GetRow 行为对齐。
func RowMapFromTableStore(raw map[string]interface{}) map[string]interface{} {
	if raw == nil {
		return nil
	}
	out := shallowCopyMap(raw)
	decodeJSONSuffixColumns(out)
	return out
}

// encodeJSONSuffixColumnsInMap 就地编码：列名以 _json 结尾时，若值非 string/[]byte/json.RawMessage，则 json.Marshal 为字符串供 OTS 存储；Marshal 失败返回错误。
func encodeJSONSuffixColumnsInMap(m map[string]interface{}) error {
	if m == nil {
		return nil
	}
	for k, v := range m {
		if !strings.HasSuffix(k, jsonColumnSuffix) {
			continue
		}
		if v == nil {
			continue
		}
		enc, err := encodeJSONCellValueForTableStore(k, v)
		if err != nil {
			return err
		}
		m[k] = enc
	}
	return nil
}

// encodeJSONCellValueForTableStore 返回可写入 OTS 的单元格值（*_json 列且结构化值→JSON 字符串）。
func encodeJSONCellValueForTableStore(columnName string, v interface{}) (interface{}, error) {
	if !strings.HasSuffix(columnName, jsonColumnSuffix) {
		return v, nil
	}
	if v == nil {
		return nil, nil
	}
	switch t := v.(type) {
	case string:
		return t, nil
	case []byte:
		return string(t), nil
	case json.RawMessage:
		return string(t), nil
	default:
		b, err := json.Marshal(t)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", columnName, err)
		}
		return string(b), nil
	}
}

// PrepareRowMapForTableStore 浅拷贝 data，将列名以 _json 结尾且值为 map/slice 等的字段序列化为字符串，便于自行组装 PutRowChange 时与 PutRow 行为一致。
func PrepareRowMapForTableStore(data map[string]interface{}) (map[string]interface{}, error) {
	out := shallowCopyMap(data)
	if err := encodeJSONSuffixColumnsInMap(out); err != nil {
		return nil, err
	}
	return out, nil
}
