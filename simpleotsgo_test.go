package simpleotsgo

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/izut/simple-ots-go/config"
)

func TestDefaultTablesConfigPath(t *testing.T) {
	// 清理环境变量，验证默认路径逻辑。
	_ = os.Unsetenv("SIMPLEOTSGO_TABLES_PATH")
	if got := DefaultTablesConfigPath(); got != filepath.Join("config", "tables.yaml") {
		t.Fatalf("default path mismatch, got=%s", got)
	}

	// 设置环境变量，验证优先级逻辑。
	want := "/tmp/custom_tables.yaml"
	_ = os.Setenv("SIMPLEOTSGO_TABLES_PATH", want)
	defer os.Unsetenv("SIMPLEOTSGO_TABLES_PATH")
	if got := DefaultTablesConfigPath(); got != want {
		t.Fatalf("env path mismatch, got=%s want=%s", got, want)
	}
}

func TestConvertTablesConfig(t *testing.T) {
	// 构造最小配置，验证类型映射与字段拷贝。
	cfg := &config.TablesConfig{
		Tables: []config.TableConfig{
			{
				Name:         "user",
				InstanceName: "test-instance",
				PrimaryKeys: []config.PrimaryKey{
					{Name: "uid", Type: "STRING"},
				},
				DefinedColumns: []config.DefinedColumn{
					{Name: "status", Type: "int"},
					{Name: "email", Type: "STRING"},
				},
			},
		},
	}

	converted, err := ConvertTablesConfig(cfg)
	if err != nil {
		t.Fatalf("convert failed: %v", err)
	}
	tc, ok := converted["user"]
	if !ok {
		t.Fatalf("missing table config user")
	}
	if tc.InstanceName != "test-instance" {
		t.Fatalf("instance mismatch: %s", tc.InstanceName)
	}
	if len(tc.PrimaryKey) != 1 || tc.PrimaryKey[0].ColumnName != "uid" {
		t.Fatalf("primary key mapping invalid: %+v", tc.PrimaryKey)
	}
	if len(tc.DefinedColumns) != 2 {
		t.Fatalf("defined columns mapping invalid: %+v", tc.DefinedColumns)
	}
}

func TestInitFromTablesYAMLWithCustomPath(t *testing.T) {
	// 生成临时 YAML 配置文件，验证自定义路径可被正确加载。
	tempDir := t.TempDir()
	yamlPath := filepath.Join(tempDir, "tables.yaml")
	content := []byte(`tables:
  - name: demo
    instanceName: i-test
    primaryKeys:
      - name: id
        type: STRING
    definedColumns:
      - name: value
        type: STRING
`)
	if err := os.WriteFile(yamlPath, content, 0o600); err != nil {
		t.Fatalf("write temp yaml failed: %v", err)
	}

	if err := initFromTablesYAML(yamlPath); err != nil {
		t.Fatalf("init from yaml failed: %v", err)
	}
}

func TestNewWithConfigValidation(t *testing.T) {
	// 验证最简入口的必填参数校验逻辑，确保错误信息明确。
	if _, err := NewWithConfig("", "sk", "https://example.com", ""); err == nil {
		t.Fatalf("expect access key required error")
	}
	if _, err := NewWithConfig("ak", "", "https://example.com", ""); err == nil {
		t.Fatalf("expect secret key required error")
	}
	// endpoint 与 area 二选一：两者都为空应报错。
	_ = os.Unsetenv("TABLESTORE_AREA")
	_ = os.Unsetenv("SIMPLEOTSGO_TABLESTORE_AREA")
	if _, err := NewWithConfig("ak", "sk", "", ""); err == nil {
		t.Fatalf("expect endpoint/area required error")
	}
}

func TestNormalizeRunMode(t *testing.T) {
	// 验证模式别名与默认值逻辑。
	if got := normalizeRunMode("prod"); got != EndpointModeProduction {
		t.Fatalf("mode mismatch: %s", got)
	}
	if got := normalizeRunMode("dev"); got != EndpointModeDevelopment {
		t.Fatalf("mode mismatch: %s", got)
	}
	if got := normalizeRunMode("unknown"); got != EndpointModeDevelopment {
		t.Fatalf("default mode mismatch: %s", got)
	}
	if got := normalizeRunMode("production"); got != EndpointModeProduction {
		t.Fatalf("production literal mismatch: %s", got)
	}
	// 模拟 New 中追加 GO_ENV：靠前变量未设置时仍能识别 production。
	if got := normalizeRunMode("", "", "", "production"); got != EndpointModeProduction {
		t.Fatalf("GO_ENV=production mismatch: %s", got)
	}
}

func TestBuildEndpoint(t *testing.T) {
	// 开发模式应拼接公网 endpoint。
	pub, err := buildEndpoint("ins", "cn-hangzhou", EndpointModeDevelopment)
	if err != nil {
		t.Fatalf("build public endpoint failed: %v", err)
	}
	if pub != "https://ins.cn-hangzhou.tablestore.aliyuncs.com" {
		t.Fatalf("public endpoint mismatch: %s", pub)
	}

	// 生产模式应拼接 VPC endpoint。
	vpc, err := buildEndpoint("ins", "cn-hangzhou", EndpointModeProduction)
	if err != nil {
		t.Fatalf("build vpc endpoint failed: %v", err)
	}
	if vpc != "https://ins.cn-hangzhou.vpc.tablestore.aliyuncs.com" {
		t.Fatalf("vpc endpoint mismatch: %s", vpc)
	}
}

func TestBuildTableStoreEndpointExported(t *testing.T) {
	// 导出函数与内部 buildEndpoint 行为一致。
	pub, err := BuildTableStoreEndpoint("ins", "cn-hangzhou", EndpointModeDevelopment)
	if err != nil || pub != "https://ins.cn-hangzhou.tablestore.aliyuncs.com" {
		t.Fatalf("exported build mismatch: %s %v", pub, err)
	}
}
