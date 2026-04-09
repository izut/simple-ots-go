package main

import (
	"testing"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
)

func TestRemoteIndexNameSet(t *testing.T) {
	s := remoteIndexNameSet(&tablestore.DescribeTableResponse{
		IndexMetas: []*tablestore.IndexMeta{
			{IndexName: "a"},
			{IndexName: "b"},
		},
	})
	if len(s) != 2 {
		t.Fatalf("got %d", len(s))
	}
	if _, ok := s["a"]; !ok {
		t.Fatal()
	}
	if len(remoteIndexNameSet(nil)) != 0 {
		t.Fatal()
	}
}
