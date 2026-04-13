package simpleotsgo

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	"github.com/izut/simple-ots-go/config"
	"github.com/izut/simple-ots-go/internal/otscore"
)

var (
	// autoInitMu 保护「当前已加载的 tables.yaml 绝对路径」与加载逻辑，避免并发下重复读或路径错乱。
	autoInitMu sync.Mutex
	// tablesConfigLoadedAbs 为已成功加载并注册到 otscore 的 YAML 绝对路径；空表示尚未成功加载。
	tablesConfigLoadedAbs string

	// defaultOperatorOnce 确保无参快捷入口 Table(...) 只初始化一次默认 Operator。
	defaultOperatorOnce sync.Once
	// defaultOperator 缓存默认 Operator，供 Table(...) 复用。
	defaultOperator *Operator
	// defaultOperatorErr 记录默认 Operator 初始化失败原因。
	defaultOperatorErr error

	// EXPECT_EXIST 表示期望存在
	EXPECT_EXIST = &tablestore.RowCondition{
		RowExistenceExpectation: tablestore.RowExistenceExpectation_EXPECT_EXIST,
	}
	// EXPECT_NOT_EXIST 表示期望不存在
	EXPECT_NOT_EXIST = &tablestore.RowCondition{
		RowExistenceExpectation: tablestore.RowExistenceExpectation_EXPECT_NOT_EXIST,
	}
	// IGNORE 表示忽略
	IGNORE = &tablestore.RowCondition{
		RowExistenceExpectation: tablestore.RowExistenceExpectation_IGNORE,
	}
)

const (
	// EndpointModeDevelopment 表示开发模式：使用公网 endpoint。
	EndpointModeDevelopment = otscore.EndpointModeDevelopment
	// EndpointModeProduction 表示生产模式：使用 VPC endpoint。
	EndpointModeProduction = otscore.EndpointModeProduction

	// FORWARD 表示正向扫描,BACKWARD 表示反向扫描
	FORWARD  = tablestore.FORWARD
	BACKWARD = tablestore.BACKWARD

	// INF_MIN 表示无穷小,INF_MAX 表示无穷大
	INF_MIN = tablestore.MIN
	INF_MAX = tablestore.MAX

	// RT_NONE 表示不返回行数据体
	RT_NONE = tablestore.ReturnType_RT_NONE
	// RT_PK 表示返回主键
	RT_PK = tablestore.ReturnType_RT_PK
	// RT_ALT 表示返回修改列
	RT_ALT = tablestore.ReturnType_RT_AFTER_MODIFY
)

// Operator 是面向业务层的简化入口。
// 设计目标是只要求 AccessKey/SecretKey，表结构与实例信息由 tables.yaml 决定。
type Operator struct {
	// endpoint 是显式指定的 TableStore 服务地址。
	// 若该值不为空，SDK 将优先使用它而不是自动拼接。
	endpoint string
	// mode 是运行模式：development 使用公网，production 使用 VPC 内网。
	mode string
	// accessKeyID 是阿里云访问密钥 ID。
	accessKeyID string
	// accessKeySecret 是阿里云访问密钥 Secret。
	accessKeySecret string
}

// TableConfig 对外暴露表配置类型，保持与核心实现一致。
type TableConfig = otscore.TableConfig

// SimpleTableOperator 对外暴露操作器类型，保持与核心实现一致。
type SimpleTableOperator = otscore.SimpleTableOperator

// Client 对外暴露客户端类型，保持与核心实现一致。
type Client = otscore.Client

// RetryConfig 对外暴露重试配置类型，保持与核心实现一致。
type RetryConfig = otscore.RetryConfig

// PKEntry 主键列项（有序），与 NewPrimaryKey 配合用于联合主键、BatchGet、GetRange 边界等。
type PKEntry = otscore.PKEntry

// GetRowOptions、BatchGetRowItem、GetRangeOptions、GetRangePage（可选的已解码分页视图）等为行级读写相关类型。
type GetRowOptions = otscore.GetRowOptions
type BatchGetRowItem = otscore.BatchGetRowItem

// UpdateData 为 UpdateRow 的入参结构体（RowData / DeleteColumns / IncrementColumns），与 internal/otscore 定义一致。
type UpdateData = otscore.UpdateData
type UpdateMutation = otscore.UpdateMutation
type ColumnVersionDelete = otscore.ColumnVersionDelete
type GetRangeOptions = otscore.GetRangeOptions
type GetRangePage = otscore.GetRangePage
type RowCondition = tablestore.RowCondition
type PrimaryKey = tablestore.PrimaryKey
type Row = tablestore.Row
type BatchWriteAction = otscore.BatchWriteAction

// NewPrimaryKey 按表定义顺序构造 *tablestore.PrimaryKey。
func NewPrimaryKey(entries ...PKEntry) *tablestore.PrimaryKey {
	return otscore.NewPrimaryKey(entries...)
}

// PrimaryKeyToMap 将 PrimaryKey 转为 map（无序；续扫分页请使用 *tablestore.PrimaryKey）。
func PrimaryKeyToMap(pk *tablestore.PrimaryKey) map[string]interface{} {
	return otscore.PrimaryKeyToMap(pk)
}

// RowToMap 将 *tablestore.Row 转为 map，并自动解析 *_json 列为对象/数组。
func RowToMap(row *tablestore.Row) map[string]interface{} {
	return otscore.RowToMap(row)
}

// BuildUpdateRowChange 由 UpdateMutation 生成 UpdateRowChange，可与 BatchWriteRowChanges 组合使用。
func BuildUpdateRowChange(table string, pk *tablestore.PrimaryKey, mut *UpdateMutation, cond *tablestore.RowCondition) (*tablestore.UpdateRowChange, error) {
	return otscore.BuildUpdateRowChange(table, pk, mut, cond)
}

// RowMapFromTableStore 将含 *_json 字符串列的 map 解析为嵌套 map/slice（与 GetRow 系列行为一致）。
func RowMapFromTableStore(raw map[string]interface{}) map[string]interface{} {
	return otscore.RowMapFromTableStore(raw)
}

// PrepareRowMapForTableStore 写入前编码 *_json 列（与 PutRow 行为一致，供直连底层 SDK 时使用）。
func PrepareRowMapForTableStore(data map[string]interface{}) (map[string]interface{}, error) {
	return otscore.PrepareRowMapForTableStore(data)
}

// Option 对外暴露客户端配置 Option 类型。
type Option = otscore.Option

// WithEndpoint 设置 Endpoint。
func WithEndpoint(endpoint string) Option { return otscore.WithEndpoint(endpoint) }

// WithInstance 设置 Instance 名称。
func WithInstance(instance string) Option { return otscore.WithInstance(instance) }

// WithAccessKey 设置 AccessKey。
func WithAccessKey(accessKey string) Option { return otscore.WithAccessKey(accessKey) }

// WithSecretKey 设置 SecretKey。
func WithSecretKey(secretKey string) Option { return otscore.WithSecretKey(secretKey) }

// NewClient 创建单实例直连客户端（适合已知 endpoint + instance 的场景）。
func NewClient(opts ...Option) (*Client, error) { return otscore.NewClient(opts...) }

// SetDefaultRetryConfig 设置全局重试配置。
func SetDefaultRetryConfig(cfg RetryConfig) { otscore.SetDefaultRetryConfig(cfg) }

// New 创建最简 SDK 操作入口。
// 1. 自动加载 tables.yaml（按默认或 SIMPLEOTSGO_TABLES_PATH 解析的绝对路径；与上次已成功加载的路径相同时跳过重复读，否则重新加载）；
// 2. 自动读取 TABLESTORE_ENDPOINT（可选；未设置时依赖各表 regionId 拼接 endpoint）；
// 3. 返回可按表名获取操作器的 Operator。
func New(accessKey, secretKey string) (*Operator, error) {
	return NewWithConfig(accessKey, secretKey, os.Getenv("TABLESTORE_ENDPOINT"), "")
}

// Table 提供“零实例化”快捷入口：用户无需显式创建 Operator，直接按表名获取操作器。
// 凭证来源：优先 TABLESTORE_ACCESS_KEY_ID/TABLESTORE_ACCESS_KEY_SECRET，回退 TABLESTORE_ACCESS_KEY/TABLESTORE_SECRET_KEY。
func Table(tableName string) (*SimpleTableOperator, error) {
	defaultOperatorOnce.Do(func() {
		ak, sk, err := loadAccessKeysFromEnv()
		if err != nil {
			defaultOperatorErr = err
			return
		}
		defaultOperator, defaultOperatorErr = New(ak, sk)
	})
	if defaultOperatorErr != nil {
		return nil, defaultOperatorErr
	}
	return defaultOperator.Table(tableName)
}

// NewWithEndpoint 在代码中显式指定 endpoint。
// 当调用方不想依赖环境变量时，建议使用该函数。
func NewWithEndpoint(accessKey, secretKey, endpoint string) (*Operator, error) {
	return NewWithConfig(accessKey, secretKey, endpoint, "")
}

// loadAccessKeysFromEnv 从环境变量读取 AK/SK（与 sync_tables 保持一致的变量优先级）。
func loadAccessKeysFromEnv() (id, secret string, err error) {
	id = strings.TrimSpace(os.Getenv("TABLESTORE_ACCESS_KEY_ID"))
	secret = strings.TrimSpace(os.Getenv("TABLESTORE_ACCESS_KEY_SECRET"))
	if id != "" && secret != "" {
		return id, secret, nil
	}
	id = strings.TrimSpace(os.Getenv("TABLESTORE_ACCESS_KEY"))
	secret = strings.TrimSpace(os.Getenv("TABLESTORE_SECRET_KEY"))
	if id != "" && secret != "" {
		return id, secret, nil
	}
	return "", "", fmt.Errorf("access key is required: set TABLESTORE_ACCESS_KEY_ID/TABLESTORE_ACCESS_KEY_SECRET or TABLESTORE_ACCESS_KEY/TABLESTORE_SECRET_KEY")
}

// NewWithConfig 创建可完全自定义初始化参数的入口。
// tablesPath 为空时会走 DefaultTablesConfigPath；不为空时使用传入路径。
// 与当前进程已成功注册的 YAML 绝对路径不一致时会重新读取并替换全局表配置（同进程多文件场景）。
func NewWithConfig(accessKey, secretKey, endpoint, tablesPath string) (*Operator, error) {
	accessKey = strings.TrimSpace(accessKey)
	secretKey = strings.TrimSpace(secretKey)
	endpoint = strings.TrimSpace(endpoint)
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
	// 未显式设置 endpoint 时，每张表须在 tables.yaml 中配置 regionId，由 Table(...) 解析 endpoint 时读取。
	// 当 tablesPath 与当前进程已加载的 YAML 不一致时重新加载，以支持同进程多配置或测试切换。
	absPath := normalizeTablesYAMLPath(tablesPath)
	autoInitMu.Lock()
	if tablesConfigLoadedAbs != absPath {
		if err := loadTablesYAML(absPath); err != nil {
			autoInitMu.Unlock()
			return nil, err
		}
		tablesConfigLoadedAbs = absPath
	}
	autoInitMu.Unlock()

	return &Operator{
		endpoint:        endpoint,
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
	return otscore.Table(tableName, endpoint, op.accessKeyID, op.accessKeySecret)
}

// resolveEndpoint 根据配置决定最终 endpoint。
// 优先级：显式 endpoint > 自动拼接（instanceName + tables.yaml 中该表的 regionId + mode）；地域不再从环境变量读取。
func (op *Operator) resolveEndpoint(tableName string) (string, error) {
	if op.endpoint != "" {
		return op.endpoint, nil
	}
	cfg, err := otscore.GetRegisteredTableConfig(tableName)
	if err != nil {
		return "", err
	}
	rid := strings.TrimSpace(cfg.RegionId)
	if rid == "" {
		return "", fmt.Errorf("table %q: regionId is required in tables.yaml when TABLESTORE_ENDPOINT is not set", tableName)
	}
	return otscore.BuildEndpoint(cfg.InstanceName, rid, op.mode)
}

// normalizeRunMode 归一化运行模式。
// 支持 development/dev 与 production/prod，默认 development。
func normalizeRunMode(candidates ...string) string {
	return otscore.NormalizeRunMode(candidates...)
}

// BuildTableStoreEndpoint 根据实例名、地域 ID（与 YAML regionId 一致）与运行模式拼接 TableStore endpoint。
// 供同步工具等场景与 SDK 内部共用，规则与 New 系列一致。
func BuildTableStoreEndpoint(instanceName, regionID, mode string) (string, error) {
	return otscore.BuildEndpoint(instanceName, regionID, mode)
}

// SyncTableStoreEndpoint 解析同步工具连接用的 endpoint。
// 若已设置 TABLESTORE_ENDPOINT，则全实例共用该地址（多实例 YAML 时请慎用）；
// 否则须由调用方传入从 tables.yaml 解析出的 regionId，并结合运行模式拼接。
func SyncTableStoreEndpoint(instanceName string) (string, error) {
	return SyncTableStoreEndpointWithRegionId(instanceName, "")
}

// SyncTableStoreEndpointWithRegionId 解析同步工具连接 endpoint；regionID 须来自 tables.yaml（同实例下各表 regionId 应一致）。
// 优先级：TABLESTORE_ENDPOINT > 传入 regionID（环境变量不再作为地域来源）。
func SyncTableStoreEndpointWithRegionId(instanceName, regionID string) (string, error) {
	return otscore.ResolveSyncEndpointWithRegionId(instanceName, regionID)
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

// normalizeTablesYAMLPath 将入参规范为用于比较与读文件的绝对路径（空串则使用默认路径）。
func normalizeTablesYAMLPath(tablesPath string) string {
	path := strings.TrimSpace(tablesPath)
	if path == "" {
		path = DefaultTablesConfigPath()
	}
	cleanPath := filepath.Clean(path)
	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		// 无法 Abs 时退回 Clean 后的路径，仍可与同字符串的后续请求对齐。
		return filepath.Clean(cleanPath)
	}
	return filepath.Clean(absPath)
}

// loadTablesYAML 读取指定路径的 YAML 并注册到 otscore（不修改 tablesConfigLoadedAbs，由调用方在成功后更新）。
func loadTablesYAML(absPath string) error {
	cfg, err := config.LoadTablesConfig(absPath)
	if err != nil {
		return fmt.Errorf("load tables config failed: %w", err)
	}
	mapped, err := ConvertTablesConfig(cfg)
	if err != nil {
		return err
	}
	return otscore.InitSimpleOperator(mapped)
}

// initFromTablesYAML 从 tables.yaml 初始化所有表配置，并记录当前已加载的绝对路径。
// 供测试或与 NewWithConfig 并列的显式初始化场景使用。
func initFromTablesYAML(path string) error {
	absPath := normalizeTablesYAMLPath(path)
	if err := loadTablesYAML(absPath); err != nil {
		return err
	}
	autoInitMu.Lock()
	tablesConfigLoadedAbs = absPath
	autoInitMu.Unlock()
	return nil
}

// ConvertTablesConfig 将 config 包中的 YAML 结构转换为运行时表配置。
// 转换过程中会校验字段类型是否合法，确保错误尽早暴露在初始化阶段。
func ConvertTablesConfig(cfg *config.TablesConfig) (map[string]*TableConfig, error) {
	return otscore.ConvertTablesConfig(cfg)
}

// ResetForTesting 重置所有全局缓存与初始化状态，仅供测试使用。
// 生产代码不应调用此函数；它使同一进程内的不同测试用例可以独立初始化。
func ResetForTesting() {
	autoInitMu.Lock()
	tablesConfigLoadedAbs = ""
	autoInitMu.Unlock()
	defaultOperatorOnce = sync.Once{}
	defaultOperator = nil
	defaultOperatorErr = nil
	otscore.ResetForTesting()
}
