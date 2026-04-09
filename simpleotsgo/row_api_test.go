package simpleotsgo

import "testing"

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
