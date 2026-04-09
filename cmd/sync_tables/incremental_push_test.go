package main

import (
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	"github.com/izut/simple-ots-go/config"
)

func strPtr(s string) *string { return &s }

func pkTypePtr(t tablestore.PrimaryKeyType) *tablestore.PrimaryKeyType { return &t }

func TestDiffDefinedColumnsToAdd(t *testing.T) {
	remote := []*tablestore.DefinedColumnSchema{
		{Name: "a", ColumnType: tablestore.DefinedColumn_STRING},
	}
	local := []config.DefinedColumn{
		{Name: "a", Type: "STRING"},
		{Name: "b", Type: "INTEGER"},
	}
	add, err := diffDefinedColumnsToAdd(remote, local)
	if err != nil {
		t.Fatal(err)
	}
	if len(add) != 1 || add[0].Name != "b" {
		t.Fatalf("got %+v", add)
	}
}

func TestDiffDefinedColumnsToAddTypeConflict(t *testing.T) {
	remote := []*tablestore.DefinedColumnSchema{
		{Name: "x", ColumnType: tablestore.DefinedColumn_INTEGER},
	}
	local := []config.DefinedColumn{
		{Name: "x", Type: "STRING"},
	}
	_, err := diffDefinedColumnsToAdd(remote, local)
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestAssertPrimaryKeysMatchRemote(t *testing.T) {
	desc := &tablestore.DescribeTableResponse{
		TableMeta: &tablestore.TableMeta{
			SchemaEntry: []*tablestore.PrimaryKeySchema{
				{Name: strPtr("pk"), Type: pkTypePtr(tablestore.PrimaryKeyType_STRING)},
			},
			DefinedColumns: nil,
		},
	}
	tc := config.TableConfig{
		PrimaryKeys: []config.PrimaryKey{{Name: "pk", Type: "STRING"}},
	}
	if err := assertPrimaryKeysMatchRemote(desc, tc); err != nil {
		t.Fatal(err)
	}
	intDesc := &tablestore.DescribeTableResponse{
		TableMeta: &tablestore.TableMeta{
			SchemaEntry: []*tablestore.PrimaryKeySchema{
				{Name: strPtr("pk"), Type: pkTypePtr(tablestore.PrimaryKeyType_INTEGER)},
			},
		},
	}
	if err := assertPrimaryKeysMatchRemote(intDesc, config.TableConfig{
		PrimaryKeys: []config.PrimaryKey{{Name: "pk", Type: "INT"}},
	}); err != nil {
		t.Fatal(err)
	}
	// 列数不一致
	if err := assertPrimaryKeysMatchRemote(desc, config.TableConfig{
		PrimaryKeys: []config.PrimaryKey{
			{Name: "a", Type: "STRING"},
			{Name: "b", Type: "STRING"},
		},
	}); err == nil {
		t.Fatal("expected error")
	}
}
