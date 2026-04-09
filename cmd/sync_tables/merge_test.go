package main

import (
	"testing"

	"github.com/izut/simple-ots-go/config"
)

// TestMergePullPreserveOtherInstances 验证全量拉取时仅替换已拉取实例，其它实例保留。
func TestMergePullPreserveOtherInstances(t *testing.T) {
	existing := []config.TableConfig{
		{Name: "keep_a", InstanceName: "tec05"},
		{Name: "old_bigots", InstanceName: "bigots"},
	}
	remote := []config.TableConfig{
		{Name: "from_remote", InstanceName: "bigots"},
	}
	pulled := map[string]bool{"bigots": true}

	got := mergePullPreserveOtherInstances(existing, remote, pulled)
	if len(got) != 2 {
		t.Fatalf("want 2 tables, got %d: %+v", len(got), got)
	}
	var hasTec, hasRemote bool
	for _, x := range got {
		if x.InstanceName == "tec05" && x.Name == "keep_a" {
			hasTec = true
		}
		if x.InstanceName == "bigots" && x.Name == "from_remote" {
			hasRemote = true
		}
		if x.Name == "old_bigots" {
			t.Fatalf("old bigots table should be replaced: %+v", x)
		}
	}
	if !hasTec || !hasRemote {
		t.Fatalf("merge result wrong: %+v", got)
	}
}
