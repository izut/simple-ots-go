package simpleotsgo

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/izut/simple-ots-go/config"
	"github.com/izut/simple-ots-go/internal/otscore"
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

func TestConvertTablesConfigKeepsIndexPrimaryKeyOrder(t *testing.T) {
	cfg := &config.TablesConfig{
		Tables: []config.TableConfig{
			{
				Name:         "task_log",
				InstanceName: "test-instance",
				PrimaryKeys: []config.PrimaryKey{
					{Name: "task_id", Type: "STRING"},
					{Name: "timestamp", Type: "INTEGER"},
				},
				Indexes: []config.Index{
					{
						Name: "task_log_index_level",
						PrimaryKeys: []config.PrimaryKey{
							{Name: "task_id", Type: "STRING"},
							{Name: "log_level", Type: "STRING"},
							{Name: "timestamp", Type: "INTEGER"},
						},
					},
				},
			},
		},
	}

	converted, err := ConvertTablesConfig(cfg)
	if err != nil {
		t.Fatalf("convert failed: %v", err)
	}
	idx, ok := converted["task_log_index_level"]
	if !ok {
		t.Fatalf("missing index config task_log_index_level")
	}
	if len(idx.PrimaryKey) != 3 {
		t.Fatalf("index primary key count mismatch: %+v", idx.PrimaryKey)
	}
	if idx.PrimaryKey[0].ColumnName != "task_id" || idx.PrimaryKey[1].ColumnName != "log_level" || idx.PrimaryKey[2].ColumnName != "timestamp" {
		t.Fatalf("index primary key order mismatch: %+v", idx.PrimaryKey)
	}
}

func TestInitFromTablesYAMLWithCustomPath(t *testing.T) {
	ResetForTesting()
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
	cfg, err := otscore.GetRegisteredTableConfig("demo")
	if err != nil {
		t.Fatalf("GetRegisteredTableConfig: %v", err)
	}
	if cfg.TableName != "demo" {
		t.Fatalf("unexpected table: %s", cfg.TableName)
	}
}

// TestNewWithConfigSwitchesTablesYAMLPath 验证同一进程内先后使用不同 tablesPath 时会重新加载配置（不沿用首次路径）。
func TestNewWithConfigSwitchesTablesYAMLPath(t *testing.T) {
	ResetForTesting()
	dir := t.TempDir()
	tiny := func(tableName string) []byte {
		return []byte(`tables:
  - name: ` + tableName + `
    instanceName: ins-x
    regionId: cn-hangzhou
    primaryKeys:
      - name: id
        type: STRING
`)
	}
	p1 := filepath.Join(dir, "one.yaml")
	p2 := filepath.Join(dir, "two.yaml")
	if err := os.WriteFile(p1, tiny("alpha"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p2, tiny("beta"), 0o600); err != nil {
		t.Fatal(err)
	}
	ep := "https://ins-x.cn-hangzhou.tablestore.aliyuncs.com"
	if _, err := NewWithConfig("ak", "sk", ep, p1); err != nil {
		t.Fatalf("first NewWithConfig: %v", err)
	}
	if _, err := otscore.GetRegisteredTableConfig("alpha"); err != nil {
		t.Fatalf("expect alpha: %v", err)
	}
	if _, err := NewWithConfig("ak", "sk", ep, p2); err != nil {
		t.Fatalf("second NewWithConfig: %v", err)
	}
	if _, err := otscore.GetRegisteredTableConfig("beta"); err != nil {
		t.Fatalf("expect beta after switch: %v", err)
	}
	if _, err := otscore.GetRegisteredTableConfig("alpha"); err == nil {
		t.Fatalf("alpha 不应在切换 YAML 后仍存在")
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
}

// TestResolveEndpointRequiresRegionIdInYAML 验证未设 TABLESTORE_ENDPOINT 时，表配置缺少 regionId 会在 Table() 阶段报错。
func TestResolveEndpointRequiresRegionIdInYAML(t *testing.T) {
	ResetForTesting()
	tempDir := t.TempDir()
	yamlPath := filepath.Join(tempDir, "tables.yaml")
	content := []byte(`tables:
  - name: no_region_table
    instanceName: ins-test
    primaryKeys:
      - name: id
        type: STRING
`)
	if err := os.WriteFile(yamlPath, content, 0o600); err != nil {
		t.Fatalf("write yaml: %v", err)
	}
	op, err := NewWithConfig("ak", "sk", "", yamlPath)
	if err != nil {
		t.Fatalf("NewWithConfig: %v", err)
	}
	_, err = op.Table("no_region_table")
	if err == nil {
		t.Fatalf("expect error when regionId missing in tables.yaml")
	}
	if !strings.Contains(err.Error(), "regionId") {
		t.Fatalf("unexpected error: %v", err)
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
	pub, err := BuildTableStoreEndpoint("ins", "cn-hangzhou", EndpointModeDevelopment)
	if err != nil {
		t.Fatalf("build public endpoint failed: %v", err)
	}
	if pub != "https://ins.cn-hangzhou.tablestore.aliyuncs.com" {
		t.Fatalf("public endpoint mismatch: %s", pub)
	}

	// 生产模式应拼接 VPC endpoint。
	vpc, err := BuildTableStoreEndpoint("ins", "cn-hangzhou", EndpointModeProduction)
	if err != nil {
		t.Fatalf("build vpc endpoint failed: %v", err)
	}
	if vpc != "https://ins.cn-hangzhou.vpc.tablestore.aliyuncs.com" {
		t.Fatalf("vpc endpoint mismatch: %s", vpc)
	}
}

func TestBuildTableStoreEndpointExported(t *testing.T) {
	// 导出函数 BuildTableStoreEndpoint 与 Operator/New 系列 endpoint 拼接规则一致。
	pub, err := BuildTableStoreEndpoint("ins", "cn-hangzhou", EndpointModeDevelopment)
	if err != nil || pub != "https://ins.cn-hangzhou.tablestore.aliyuncs.com" {
		t.Fatalf("exported build mismatch: %s %v", pub, err)
	}
}
