package simpleotsgo

import (
	"fmt"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
)

// TableConfig 表配置结构
type TableConfig struct {
	TableName      string
	InstanceName   string
	PrimaryKey     []tablestore.PrimaryKeyColumn
	DefinedColumns []tablestore.AttributeColumn
}

// SimpleTableOperator 绑定单张表与 TableStore 客户端，行级 API 命名与官方一致：GetRow / PutRow / UpdateRow / DeleteRow、BatchGetRows / BatchPutRows、BatchWriteChanges。
type SimpleTableOperator struct {
	tableName string
	client    *tablestore.TableStoreClient
	config    *TableConfig
}

// 全局缓存
var (
	// operators 缓存已创建的表操作器
	operators   = make(map[string]*SimpleTableOperator)
	operatorsMu sync.RWMutex

	// clients 缓存客户端连接
	clients   = make(map[string]*tablestore.TableStoreClient)
	clientsMu sync.RWMutex

	// tableConfigs 缓存表配置
	tableConfigs   = make(map[string]*TableConfig)
	tableConfigsMu sync.RWMutex

	// initialized 初始化状态
	initialized bool
	initMu      sync.Mutex
)

// buildClientCacheKey 构建客户端缓存键。
// 将 endpoint、instance 和 accessKey 组合，避免不同账号或环境发生缓存串用。
func buildClientCacheKey(instanceName, endpoint, accessKey string) string {
	return strings.Join([]string{endpoint, instanceName, accessKey}, "|")
}

// buildOperatorCacheKey 构建表操作器缓存键。
// 同一表在不同 endpoint 或账号下应视为不同操作器实例。
func buildOperatorCacheKey(tableName, endpoint, accessKey string) string {
	return strings.Join([]string{tableName, endpoint, accessKey}, "|")
}

// InitSimpleOperator 初始化操作器
func InitSimpleOperator(configs map[string]*TableConfig) error {
	initMu.Lock()
	defer initMu.Unlock()

	if initialized {
		return nil
	}

	// 注册所有表配置
	tableConfigsMu.Lock()
	tableConfigs = configs
	tableConfigsMu.Unlock()

	initialized = true
	return nil
}

// RegisterTable 手动注册表配置
func RegisterTable(tableConfig *TableConfig) {
	tableConfigsMu.Lock()
	defer tableConfigsMu.Unlock()
	tableConfigs[tableConfig.TableName] = tableConfig
}

// getClient 获取指定实例的客户端（带缓存）
func getClient(instanceName, endpoint, accessKey, secretKey string) (*tablestore.TableStoreClient, error) {
	cacheKey := buildClientCacheKey(instanceName, endpoint, accessKey)

	// 读锁检查缓存
	clientsMu.RLock()
	client, exists := clients[cacheKey]
	clientsMu.RUnlock()

	if exists {
		return client, nil
	}

	// 写锁创建客户端
	clientsMu.Lock()
	defer clientsMu.Unlock()

	// 双重检查
	if client, exists := clients[cacheKey]; exists {
		return client, nil
	}

	// 创建新客户端
	client = tablestore.NewClient(
		endpoint,
		instanceName,
		accessKey,
		secretKey,
	)

	clients[cacheKey] = client
	return client, nil
}

// Table 获取表操作器
func Table(tableName, endpoint, accessKey, secretKey string) (*SimpleTableOperator, error) {
	operatorCacheKey := buildOperatorCacheKey(tableName, endpoint, accessKey)

	// 1. 查找表配置
	tableConfigsMu.RLock()
	tableConfig, exists := tableConfigs[tableName]
	tableConfigsMu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("table config not found: %s", tableName)
	}

	// 2. 检查操作器缓存
	operatorsMu.RLock()
	operator, exists := operators[operatorCacheKey]
	operatorsMu.RUnlock()

	if exists {
		return operator, nil
	}

	// 3. 获取对应实例的客户端
	client, err := getClient(tableConfig.InstanceName, endpoint, accessKey, secretKey)
	if err != nil {
		return nil, fmt.Errorf("failed to get client for instance %s: %w", tableConfig.InstanceName, err)
	}

	// 4. 创建新的操作器
	operatorsMu.Lock()
	defer operatorsMu.Unlock()

	// 双重检查
	if operator, exists := operators[operatorCacheKey]; exists {
		return operator, nil
	}

	operator = &SimpleTableOperator{
		tableName: tableName,
		client:    client,
		config:    tableConfig,
	}
	operators[operatorCacheKey] = operator

	return operator, nil
}

// buildPrimaryKey 根据数据构建主键
func (op *SimpleTableOperator) buildPrimaryKey(data map[string]interface{}) (*tablestore.PrimaryKey, error) {
	pk := &tablestore.PrimaryKey{}
	for _, pkCol := range op.config.PrimaryKey {
		val, exists := data[pkCol.ColumnName]
		if !exists {
			return nil, fmt.Errorf("missing primary key: %s", pkCol.ColumnName)
		}
		pk.AddPrimaryKeyColumn(pkCol.ColumnName, val)
	}
	return pk, nil
}

// GetRow 按主键读取一行。opts 为 nil 时等价于全列读取、MaxVersion=1。
// 主键字段须与 tables.yaml 中 primaryKeys 列名一致；含 ColumnsToGet 时可做列裁剪。
func (op *SimpleTableOperator) GetRow(primaryKeys map[string]interface{}, opts *GetRowOptions) (map[string]interface{}, error) {
	pk, err := op.buildPrimaryKey(primaryKeys)
	if err != nil {
		return nil, err
	}
	return op.tsClient().GetRowWithOptions(op.tableName, pk, opts)
}

// BatchGetRows 对当前表批量主键读取，每行 map 须包含完整主键列。
func (op *SimpleTableOperator) BatchGetRows(rows []map[string]interface{}, opts *GetRowOptions) ([]BatchGetRowItem, error) {
	keys := make([]*tablestore.PrimaryKey, 0, len(rows))
	for _, r := range rows {
		pk, err := op.buildPrimaryKey(r)
		if err != nil {
			return nil, err
		}
		keys = append(keys, pk)
	}
	return op.tsClient().BatchGetRowSameTable(op.tableName, keys, opts)
}

// BatchWriteChanges 执行 BatchWriteRow：可同时提交 PutRowChange / UpdateRowChange / DeleteRowChange。
// 每个变更内的 TableName 须与该表一致（跨表批量时请使用 *Client.BatchWriteRowChanges）。
func (op *SimpleTableOperator) BatchWriteChanges(changes []tablestore.RowChange) error {
	return op.tsClient().BatchWriteRowChanges(changes)
}

// UpdateRowWithMutation 使用 UpdateMutation 更新一行（Put / 删列 / 自增 / 按版本删列）。
func (op *SimpleTableOperator) UpdateRowWithMutation(primaryKeys map[string]interface{}, mut *UpdateMutation, cond *tablestore.RowCondition) error {
	pk, err := op.buildPrimaryKey(primaryKeys)
	if err != nil {
		return err
	}
	return op.tsClient().UpdateRowWithMutation(op.tableName, pk, mut, cond)
}

// GetRangeWithPrimaryKeys 使用有序主键边界范围扫描，支持 ColumnToGet、反向、Limit 与 next_start_pk 分页。
func (op *SimpleTableOperator) GetRangeWithPrimaryKeys(startPK, endPK *tablestore.PrimaryKey, opts *GetRangeOptions) (*GetRangePage, error) {
	return op.tsClient().GetRangeWithOptions(op.tableName, startPK, endPK, opts)
}

// PutRow 写入或覆盖一行（与 TableStore PutRow 语义一致）。data 须含全部主键列；*_json 列会以 JSON 字符串形式写入。
func (op *SimpleTableOperator) PutRow(data map[string]interface{}) error {
	putRowChange, err := op.assemblePutRowChangeFromMap(data)
	if err != nil {
		return err
	}
	req := &tablestore.PutRowRequest{PutRowChange: putRowChange}
	err = withRetry(func() error {
		_, innerErr := op.client.PutRow(req)
		return innerErr
	})
	if err != nil {
		return fmt.Errorf("put row failed: %w", err)
	}
	return nil
}

// UpdateRow 按主键更新指定属性列（行须已存在）。
func (op *SimpleTableOperator) UpdateRow(primaryKeys map[string]interface{}, updates map[string]interface{}) error {
	pk, err := op.buildPrimaryKey(primaryKeys)
	if err != nil {
		return err
	}

	updateRowChange := &tablestore.UpdateRowChange{
		TableName:  op.tableName,
		PrimaryKey: pk,
		Condition: &tablestore.RowCondition{
			RowExistenceExpectation: tablestore.RowExistenceExpectation_EXPECT_EXIST,
		},
	}

	updatesForOTS := shallowCopyMap(updates)
	if err := encodeJSONSuffixColumnsInMap(updatesForOTS); err != nil {
		return err
	}
	for colName, val := range updatesForOTS {
		updateRowChange.PutColumn(colName, val)
	}

	req := &tablestore.UpdateRowRequest{
		UpdateRowChange: updateRowChange,
	}

	err = withRetry(func() error {
		_, innerErr := op.client.UpdateRow(req)
		return innerErr
	})
	if err != nil {
		return fmt.Errorf("update row failed: %w", err)
	}
	return nil
}

// DeleteRow 按主键删除一行。
func (op *SimpleTableOperator) DeleteRow(primaryKeys map[string]interface{}) error {
	pk, err := op.buildPrimaryKey(primaryKeys)
	if err != nil {
		return err
	}

	deleteRowChange := &tablestore.DeleteRowChange{
		TableName:  op.tableName,
		PrimaryKey: pk,
		Condition: &tablestore.RowCondition{
			RowExistenceExpectation: tablestore.RowExistenceExpectation_IGNORE,
		},
	}

	req := &tablestore.DeleteRowRequest{
		DeleteRowChange: deleteRowChange,
	}

	err = withRetry(func() error {
		_, innerErr := op.client.DeleteRow(req)
		return innerErr
	})
	if err != nil {
		return fmt.Errorf("delete row failed: %w", err)
	}
	return nil
}

// BatchPutRows 批量 PutRow，每行语义与 PutRow 相同。
func (op *SimpleTableOperator) BatchPutRows(rows []map[string]interface{}) error {
	batchRequest := &tablestore.BatchWriteRowRequest{
		RowChangesGroupByTable: make(map[string][]tablestore.RowChange),
	}
	for _, row := range rows {
		putRowChange, err := op.assemblePutRowChangeFromMap(row)
		if err != nil {
			return err
		}
		batchRequest.RowChangesGroupByTable[op.tableName] = append(
			batchRequest.RowChangesGroupByTable[op.tableName],
			putRowChange,
		)
	}
	err := withRetry(func() error {
		_, innerErr := op.client.BatchWriteRow(batchRequest)
		return innerErr
	})
	if err != nil {
		return fmt.Errorf("batch put rows failed: %w", err)
	}
	return nil
}

// GetRange 范围查询（前闭后开等行为以 OTS 文档为准）。边界 map 的列顺序依赖 Go map 遍历，联合主键场景请优先使用 GetRangeWithPrimaryKeys。
func (op *SimpleTableOperator) GetRange(startPK, endPK map[string]interface{}, limit int32) ([]map[string]interface{}, error) {
	// 构建起始主键
	startPrimaryKey := &tablestore.PrimaryKey{}
	for k, v := range startPK {
		startPrimaryKey.AddPrimaryKeyColumn(k, v)
	}

	// 构建结束主键
	endPrimaryKey := &tablestore.PrimaryKey{}
	for k, v := range endPK {
		endPrimaryKey.AddPrimaryKeyColumn(k, v)
	}

	criteria := &tablestore.RangeRowQueryCriteria{
		TableName:       op.tableName,
		StartPrimaryKey: startPrimaryKey,
		EndPrimaryKey:   endPrimaryKey,
		Direction:       tablestore.FORWARD,
		Limit:           limit,
		MaxVersion:      1,
	}

	req := &tablestore.GetRangeRequest{
		RangeRowQueryCriteria: criteria,
	}

	var resp *tablestore.GetRangeResponse
	err := withRetry(func() error {
		var innerErr error
		resp, innerErr = op.client.GetRange(req)
		return innerErr
	})
	if err != nil {
		return nil, fmt.Errorf("get range failed: %w", err)
	}

	results := make([]map[string]interface{}, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		results = append(results, tableStoreRowToDecodedMap(row))
	}
	return results, nil
}

// GetTableName 获取表名
func (op *SimpleTableOperator) GetTableName() string {
	return op.tableName
}

// GetInstanceName 获取实例名
func (op *SimpleTableOperator) GetInstanceName() string {
	return op.config.InstanceName
}

// GetAllRegisteredTables 获取所有已注册的表名
func GetAllRegisteredTables() []string {
	tableConfigsMu.RLock()
	defer tableConfigsMu.RUnlock()

	tables := make([]string, 0, len(tableConfigs))
	for tableName := range tableConfigs {
		tables = append(tables, tableName)
	}
	return tables
}

// GetRegisteredTableConfig 根据表名读取已注册配置。
// 该方法返回的是只读配置指针，调用方不应修改其内部字段。
func GetRegisteredTableConfig(tableName string) (*TableConfig, error) {
	tableConfigsMu.RLock()
	defer tableConfigsMu.RUnlock()

	cfg, ok := tableConfigs[tableName]
	if !ok {
		return nil, fmt.Errorf("table config not found: %s", tableName)
	}
	return cfg, nil
}
