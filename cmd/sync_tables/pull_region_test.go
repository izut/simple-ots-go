package main

import "testing"

// TestEffectiveRegionForPull 验证拉取时地域解析：YAML 优先，其次为 -regionId 回退。
func TestEffectiveRegionForPull(t *testing.T) {
	by := map[string]string{"insA": " cn-hangzhou "}
	if got := effectiveRegionForPull("insA", by, "cn-beijing"); got != "cn-hangzhou" {
		t.Fatalf("期望 YAML 优先，得到 %q", got)
	}
	if got := effectiveRegionForPull("insB", by, "cn-shanghai"); got != "cn-shanghai" {
		t.Fatalf("期望命令行回退，得到 %q", got)
	}
	if got := effectiveRegionForPull("insB", map[string]string{}, ""); got != "" {
		t.Fatalf("期望空字符串，得到 %q", got)
	}
}
