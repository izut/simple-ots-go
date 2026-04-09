package simpleotsgo

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	"github.com/izut/simple-ots-go/config"
	core "github.com/izut/simple-ots-go/simpleotsgo"
)

var (
	// autoInitOnce 确保 YAML 自动初始化逻辑只执行一次，避免并发重复初始化。
	autoInitOnce sync.Once
	// autoInitErr 记录自动初始化结果，后续 New 调用会复用该结果。
	autoInitErr error
)

const (
	// EndpointModeDevelopment 表示开发模式：使用公网 endpoint。
	EndpointModeDevelopment = "development"
	// EndpointModeProduction 表示生产模式：使用 VPC endpoint。
	EndpointModeProduction = "production"
)

// Operator 是面向业务层的简化入口。
// 设计目标是只要求 AccessKey/SecretKey，表结构与实例信息由 tables.yaml 决定。
type Operator struct {
	// endpoint 是显式指定的 TableStore 服务地址。
	// 若该值不为空，SDK 将优先使用它而不是自动拼接。
	endpoint string
	// area 是区域代码（如 cn-hangzhou），用于自动拼接 endpoint。
	area string
	// mode 是运行模式：development 使用公网，production 使用 VPC 内网。
	mode string
	// accessKeyID 是阿里云访问密钥 ID。
	accessKeyID string
	// accessKeySecret 是阿里云访问密钥 Secret。
	accessKeySecret string
}

// TableConfig 对外暴露表配置类型，保持与核心实现一致。
type TableConfig = core.TableConfig

// SimpleTableOperator 对外暴露操作器类型，保持与核心实现一致。
type SimpleTableOperator = core.SimpleTableOperator

// Client 对外暴露客户端类型，保持与核心实现一致。
type Client = core.Client

// RetryConfig 对外暴露重试配置类型，保持与核心实现一致。
type RetryConfig = core.RetryConfig

// PKEntry 主键列项（有序），与 NewPrimaryKey 配合用于联合主键、BatchGet、GetRange 边界等。
type PKEntry = core.PKEntry

// GetRowOptions、BatchGetRowItem、GetRangeOptions、GetRangePage 等为行级读写可选参数与结果类型。
type GetRowOptions = core.GetRowOptions
type BatchGetRowItem = core.BatchGetRowItem
type UpdateMutation = core.UpdateMutation
type ColumnVersionDelete = core.ColumnVersionDelete
type GetRangeOptions = core.GetRangeOptions
type GetRangePage = core.GetRangePage

// NewPrimaryKey 按表定义顺序构造 *tablestore.PrimaryKey。
func NewPrimaryKey(entries ...PKEntry) *tablestore.PrimaryKey { return core.NewPrimaryKey(entries...) }

// PrimaryKeyToMap 将 PrimaryKey 转为 map（无序；续扫分页请使用 *tablestore.PrimaryKey）。
func PrimaryKeyToMap(pk *tablestore.PrimaryKey) map[string]interface{} { return core.PrimaryKeyToMap(pk) }

// BuildUpdateRowChange 由 UpdateMutation 生成 UpdateRowChange，可与 BatchWriteRowChanges 组合使用。
func BuildUpdateRowChange(table string, pk *tablestore.PrimaryKey, mut *UpdateMutation, cond *tablestore.RowCondition) (*tablestore.UpdateRowChange, error) {
	return core.BuildUpdateRowChange(table, pk, mut, cond)
}

// RowMapFromTableStore 将含 *_json 字符串列的 map 解析为嵌套 map/slice（与 GetRow 系列行为一致）。
func RowMapFromTableStore(raw map[string]interface{}) map[string]interface{} {
	return core.RowMapFromTableStore(raw)
}

// PrepareRowMapForTableStore 写入前编码 *_json 列（与 PutRow 行为一致，供直连底层 SDK 时使用）。
func PrepareRowMapForTableStore(data map[string]interface{}) (map[string]interface{}, error) {
	return core.PrepareRowMapForTableStore(data)
}

// Option 对外暴露客户端配置 Option 类型。
type Option = core.Option

// WithEndpoint 设置 Endpoint。
func WithEndpoint(endpoint string) Option { return core.WithEndpoint(endpoint) }

// WithInstance 设置 Instance 名称。
func WithInstance(instance string) Option { return core.WithInstance(instance) }

// WithAccessKey 设置 AccessKey。
func WithAccessKey(accessKey string) Option { return core.WithAccessKey(accessKey) }

// WithSecretKey 设置 SecretKey。
func WithSecretKey(secretKey string) Option { return core.WithSecretKey(secretKey) }

// NewClient 创建客户端。
func NewClient(opts ...Option) (*Client, error) { return core.NewClient(opts...) }

// SetDefaultRetryConfig 设置全局重试配置。
func SetDefaultRetryConfig(cfg RetryConfig) { core.SetDefaultRetryConfig(cfg) }

// New 创建最简 SDK 操作入口。
// 1. 自动加载 tables.yaml（只执行一次）；
// 2. 自动读取 TABLESTORE_ENDPOINT；
// 3. 返回可按表名获取操作器的 Operator。
func New(accessKey, secretKey string) (*Operator, error) {
	return NewWithConfig(accessKey, secretKey, os.Getenv("TABLESTORE_ENDPOINT"), "")
}

// NewWithEndpoint 在代码中显式指定 endpoint。
// 当调用方不想依赖环境变量时，建议使用该函数。
func NewWithEndpoint(accessKey, secretKey, endpoint string) (*Operator, error) {
	return NewWithConfig(accessKey, secretKey, endpoint, "")
}

// NewWithConfig 创建可完全自定义初始化参数的入口。
// tablesPath 为空时会走 DefaultTablesConfigPath；不为空时优先使用传入路径。
func NewWithConfig(accessKey, secretKey, endpoint, tablesPath string) (*Operator, error) {
	accessKey = strings.TrimSpace(accessKey)
	secretKey = strings.TrimSpace(secretKey)
	endpoint = strings.TrimSpace(endpoint)
	area := strings.TrimSpace(os.Getenv("TABLESTORE_AREA"))
	if area == "" {
		area = strings.TrimSpace(os.Getenv("SIMPLEOTSGO_TABLESTORE_AREA"))
	}
	mode := normalizeRunMode(
		os.Getenv("SIMPLEOTSGO_RUN_MODE"),
		os.Getenv("APP_ENV"),
		os.Getenv("ENV"),
		os.Getenv("GO_ENV"),
	)
	if accessKey == "" {
		return nil, fmt.Errorf("access key is required")
	}
	if secretKey == "" {
		return nil, fmt.Errorf("secret key is required")
	}
	// endpoint 支持两种来源：
	// 1) 显式传入/环境变量 TABLESTORE_ENDPOINT（优先）；
	// 2) 通过 area + mode + instanceName 自动拼接。
	if endpoint == "" && area == "" {
		return nil, fmt.Errorf("endpoint or area is required (set TABLESTORE_ENDPOINT or TABLESTORE_AREA)")
	}

	// 自动初始化表配置：只做一次，保证并发安全与全局一致性。
	autoInitOnce.Do(func() {
		autoInitErr = initFromTablesYAML(tablesPath)
	})
	if autoInitErr != nil {
		return nil, autoInitErr
	}

	return &Operator{
		endpoint:        endpoint,
		area:            area,
		mode:            mode,
		accessKeyID:     accessKey,
		accessKeySecret: secretKey,
	}, nil
}

// Table 根据表名获取表操作器。
// SDK 会从已加载的 tables.yaml 中解析该表的 instanceName 与主键结构。
func (op *Operator) Table(tableName string) (*SimpleTableOperator, error) {
	if op == nil {
		return nil, fmt.Errorf("operator is nil")
	}
	if strings.TrimSpace(tableName) == "" {
		return nil, fmt.Errorf("table name is required")
	}
	endpoint, err := op.resolveEndpoint(tableName)
	if err != nil {
		return nil, err
	}
	return core.Table(tableName, endpoint, op.accessKeyID, op.accessKeySecret)
}

// resolveEndpoint 根据配置决定最终 endpoint。
// 优先级：显式 endpoint > 自动拼接（instanceName + area + mode）。
func (op *Operator) resolveEndpoint(tableName string) (string, error) {
	if op.endpoint != "" {
		return op.endpoint, nil
	}
	cfg, err := core.GetRegisteredTableConfig(tableName)
	if err != nil {
		return "", err
	}
	return buildEndpoint(cfg.InstanceName, op.area, op.mode)
}

// normalizeRunMode 归一化运行模式。
// 支持 development/dev 与 production/prod，默认 development。
func normalizeRunMode(candidates ...string) string {
	for _, c := range candidates {
		switch strings.ToLower(strings.TrimSpace(c)) {
		case "production", "prod":
			return EndpointModeProduction
		case "development", "dev":
			return EndpointModeDevelopment
		}
	}
	return EndpointModeDevelopment
}

// BuildTableStoreEndpoint 根据实例名、区域与运行模式拼接 TableStore endpoint。
// 供同步工具等场景与 SDK 内部共用，规则与 New 系列一致。
func BuildTableStoreEndpoint(instanceName, area, mode string) (string, error) {
	return buildEndpoint(instanceName, area, mode)
}

// SyncTableStoreEndpoint 解析同步工具连接用的 endpoint。
// 若已设置 TABLESTORE_ENDPOINT，则全实例共用该地址（多实例 YAML 时请慎用）；
// 否则按 instanceName + TABLESTORE_AREA + 运行模式自动拼接。
func SyncTableStoreEndpoint(instanceName string) (string, error) {
	if e := strings.TrimSpace(os.Getenv("TABLESTORE_ENDPOINT")); e != "" {
		return e, nil
	}
	area := strings.TrimSpace(os.Getenv("TABLESTORE_AREA"))
	if area == "" {
		area = strings.TrimSpace(os.Getenv("SIMPLEOTSGO_TABLESTORE_AREA"))
	}
	mode := normalizeRunMode(
		os.Getenv("SIMPLEOTSGO_RUN_MODE"),
		os.Getenv("APP_ENV"),
		os.Getenv("ENV"),
		os.Getenv("GO_ENV"),
	)
	return buildEndpoint(instanceName, area, mode)
}

// buildEndpoint 按实例名、区域和运行模式拼接 endpoint。
// - development：公网域名
// - production：VPC 域名
func buildEndpoint(instanceName, area, mode string) (string, error) {
	instanceName = strings.TrimSpace(instanceName)
	area = strings.TrimSpace(area)
	if instanceName == "" {
		return "", fmt.Errorf("instance name is empty")
	}
	if area == "" {
		return "", fmt.Errorf("area is required when endpoint is not explicitly set")
	}
	if mode == EndpointModeProduction {
		return fmt.Sprintf("https://%s.%s.vpc.tablestore.aliyuncs.com", instanceName, area), nil
	}
	return fmt.Sprintf("https://%s.%s.tablestore.aliyuncs.com", instanceName, area), nil
}

// DefaultTablesConfigPath 返回默认配置路径。
// 解析顺序：
// 1. 优先读取环境变量 SIMPLEOTSGO_TABLES_PATH；
// 2. 未设置时回落到当前工作目录下的 config/tables.yaml。
func DefaultTablesConfigPath() string {
	if p := strings.TrimSpace(os.Getenv("SIMPLEOTSGO_TABLES_PATH")); p != "" {
		return p
	}
	return filepath.Join("config", "tables.yaml")
}

// initFromTablesYAML 从 tables.yaml 初始化所有表配置。
// 该方法为内部初始化流程，避免用户手写表结构映射。
func initFromTablesYAML(path string) error {
	if strings.TrimSpace(path) == "" {
		path = DefaultTablesConfigPath()
	}
	cfg, err := config.LoadTablesConfig(path)
	if err != nil {
		return fmt.Errorf("load tables config failed: %w", err)
	}
	mapped, err := ConvertTablesConfig(cfg)
	if err != nil {
		return err
	}
	return core.InitSimpleOperator(mapped)
}

// ConvertTablesConfig 将 config 包中的 YAML 结构转换为运行时表配置。
// 转换过程中会校验字段类型是否合法，确保错误尽早暴露在初始化阶段。
func ConvertTablesConfig(cfg *config.TablesConfig) (map[string]*TableConfig, error) {
	result := make(map[string]*TableConfig, len(cfg.Tables))
	for _, t := range cfg.Tables {
		pkCols := make([]tablestore.PrimaryKeyColumn, 0, len(t.PrimaryKeys))
		for _, pk := range t.PrimaryKeys {
			if _, err := parsePrimaryKeyType(pk.Type); err != nil {
				return nil, fmt.Errorf("table %s primary key %s type invalid: %w", t.Name, pk.Name, err)
			}
			// v5 SDK 的 PrimaryKeyColumn 结构用于“值表达”，
			// 这里仅保存列名用于后续主键校验。
			pkCols = append(pkCols, tablestore.PrimaryKeyColumn{
				ColumnName:       pk.Name,
				Value:            nil,
				PrimaryKeyOption: tablestore.NONE,
			})
		}

		attrCols := make([]tablestore.AttributeColumn, 0, len(t.DefinedColumns))
		for _, c := range t.DefinedColumns {
			if _, err := parseColumnType(c.Type); err != nil {
				return nil, fmt.Errorf("table %s column %s type invalid: %w", t.Name, c.Name, err)
			}
			// v5 SDK 的 AttributeColumn 在写入场景中用于“列值”，
			// 这里仅缓存字段名，Value/Timestamp 保持空值即可。
			attrCols = append(attrCols, tablestore.AttributeColumn{
				ColumnName: c.Name,
				Value:      nil,
				Timestamp:  0,
			})
		}

		result[t.Name] = &TableConfig{
			TableName:      t.Name,
			InstanceName:   t.InstanceName,
			PrimaryKey:     pkCols,
			DefinedColumns: attrCols,
		}
	}
	return result, nil
}

// parsePrimaryKeyType 解析主键类型字符串。
// 该函数用于初始化时的合法性校验，返回值仅用于验证，不参与最终列值写入。
func parsePrimaryKeyType(s string) (tablestore.PrimaryKeyType, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "STRING":
		return tablestore.PrimaryKeyType_STRING, nil
	case "INTEGER", "INT":
		return tablestore.PrimaryKeyType_INTEGER, nil
	case "BINARY":
		return tablestore.PrimaryKeyType_BINARY, nil
	default:
		return 0, fmt.Errorf("unsupported primary key type: %s", s)
	}
}

// parseColumnType 解析属性列类型字符串。
// 支持常见大小写与别名（如 INT/INTEGER、BOOL/BOOLEAN）。
func parseColumnType(s string) (tablestore.ColumnType, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "STRING":
		return tablestore.ColumnType_STRING, nil
	case "INTEGER", "INT":
		return tablestore.ColumnType_INTEGER, nil
	case "BINARY":
		return tablestore.ColumnType_BINARY, nil
	case "BOOLEAN", "BOOL":
		return tablestore.ColumnType_BOOLEAN, nil
	case "DOUBLE", "FLOAT":
		return tablestore.ColumnType_DOUBLE, nil
	default:
		return 0, fmt.Errorf("unsupported column type: %s", s)
	}
}
