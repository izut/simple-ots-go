package otscore

import (
	"reflect"
	"testing"
)

func TestDecodeJSONSuffixColumns(t *testing.T) {
	m := map[string]interface{}{
		"id":         "a",
		"payload_json": `{"x":1,"y":[2,3]}`,
	}
	decodeJSONSuffixColumns(m)
	obj, ok := m["payload_json"].(map[string]interface{})
	if !ok {
		t.Fatalf("got %T", m["payload_json"])
	}
	if obj["x"].(float64) != 1 {
		t.Fatal(obj)
	}
}

func TestEncodeJSONSuffixColumnsInMap(t *testing.T) {
	m := map[string]interface{}{
		"payload_json": map[string]interface{}{"k": "v"},
		"plain":        "stay",
	}
	if err := encodeJSONSuffixColumnsInMap(m); err != nil {
		t.Fatal(err)
	}
	if m["plain"] != "stay" {
		t.Fatal()
	}
	s, ok := m["payload_json"].(string)
	if !ok || s != `{"k":"v"}` {
		t.Fatalf("%q %v", s, ok)
	}
}

func TestPrepareRowMapForTableStore(t *testing.T) {
	src := map[string]interface{}{"x_json": map[string]int{"a": 1}}
	out, err := PrepareRowMapForTableStore(src)
	if err != nil {
		t.Fatal(err)
	}
	if src["x_json"] == nil {
		t.Fatal("source must not be mutated to nil")
	}
	if _, ok := out["x_json"].(string); !ok {
		t.Fatalf("%T", out["x_json"])
	}
}

func TestRowMapFromTableStoreCopy(t *testing.T) {
	raw := map[string]interface{}{"meta_data_json": `{"n":1}`}
	out := RowMapFromTableStore(raw)
	if reflect.DeepEqual(raw, out) {
		t.Fatal("expected decoded copy")
	}
	if raw["meta_data_json"] != `{"n":1}` {
		t.Fatal("original should be untouched")
	}
}
