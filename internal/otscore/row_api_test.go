package otscore

import (
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
)

func TestBuildUpdateRowChange_EmptyMutationErrors(t *testing.T) {
	pk := NewPrimaryKey(PKEntry{Name: "k", Value: "v"})
	_, err := BuildUpdateRowChange("t", pk, &UpdateMutation{}, nil)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestBuildUpdateRowChange_PutOK(t *testing.T) {
	pk := NewPrimaryKey(PKEntry{Name: "k", Value: "v"})
	ch, err := BuildUpdateRowChange("t", pk, &UpdateMutation{Put: map[string]interface{}{"a": int64(1)}}, nil)
	if err != nil || ch == nil || len(ch.Columns) == 0 {
		t.Fatalf("ch=%v err=%v", ch, err)
	}
}

func TestPrimaryKeyToMap(t *testing.T) {
	pk := NewPrimaryKey(PKEntry{Name: "a", Value: "1"}, PKEntry{Name: "b", Value: int64(2)})
	m := PrimaryKeyToMap(pk)
	if m["a"] != "1" || m["b"] != int64(2) {
		t.Fatalf("%v", m)
	}
}

func TestRowToMap_DecodeJSONSuffix(t *testing.T) {
	row := &tablestore.Row{
		PrimaryKey: NewPrimaryKey(PKEntry{Name: "uid", Value: "u1"}),
		Columns: []*tablestore.AttributeColumn{
			{ColumnName: "meta_data_json", Value: `{"k":"v","n":1}`},
			{ColumnName: "plain", Value: "ok"},
		},
	}
	m := RowToMap(row)
	if m["uid"] != "u1" || m["plain"] != "ok" {
		t.Fatalf("unexpected row map: %+v", m)
	}
	obj, ok := m["meta_data_json"].(map[string]interface{})
	if !ok {
		t.Fatalf("meta_data_json should be decoded object, got %T", m["meta_data_json"])
	}
	if obj["k"] != "v" {
		t.Fatalf("decoded json mismatch: %+v", obj)
	}
}
