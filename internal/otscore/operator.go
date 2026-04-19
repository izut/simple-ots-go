package otscore

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
)

// TableConfig 表配置结构
type TableConfig struct {
	TableName    string
	InstanceName string
	// RegionId 与 tables.yaml 中 regionId 对应，用于与 instanceName、运行模式拼接 endpoint。
	RegionId       string
	PrimaryKey     []tablestore.PrimaryKeyColumn
	DefinedColumns []tablestore.AttributeColumn
}

// SimpleTableOperator 绑定单张表与 TableStore 客户端，行级 API 命名与官方一致：
// GetRow（列裁剪 + MaxVersion）/ PutRow（可传 RowCondition，默认 ReturnType=RT_PK，返回 PutRowResponse）/ UpdateRow（UpdateData：Put + 可选删列/自增，可传 RowCondition，返回 UpdateRowResponse）/ DeleteRow（返回 DeleteRowResponse）、BatchGetRows（列裁剪，MaxVersion 固定 1）/ BatchPutRows（全行共用 Condition + ReturnType，返回 *BatchWriteRowResponse）/ BatchWriteChanges。
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

	initMu sync.Mutex
)

// buildOperatorCacheKey 构建表操作器缓存键。
// 同一表在不同 endpoint 或账号下应视为不同操作器实例。
func buildOperatorCacheKey(tableName, endpoint, accessKey string) string {
	return strings.Join([]string{tableName, endpoint, accessKeyFingerprint(accessKey)}, "|")
}

// accessKeyFingerprint 返回 accessKey 的稳定短指纹（sha256 前 16 hex），仅用于缓存键区分账号，不用于安全校验。
func accessKeyFingerprint(accessKey string) string {
	sum := sha256.Sum256([]byte(accessKey))
	return hex.EncodeToString(sum[:8])
}

// InitSimpleOperator 用新的表配置替换全局注册表，并清空客户端与操作器缓存。
// 同一进程内切换不同 tables.yaml（或测试隔离）时依赖此行为，而非「仅首次生效」。
func InitSimpleOperator(configs map[string]*TableConfig) error {
	initMu.Lock()
	defer initMu.Unlock()

	// 切换配置后必须丢弃旧缓存，否则旧 SimpleTableOperator 仍持有已脱离全局 map 的 *TableConfig 指针。
	operatorsMu.Lock()
	operators = make(map[string]*SimpleTableOperator)
	operatorsMu.Unlock()

	clientsMu.Lock()
	clients = make(map[string]*tablestore.TableStoreClient)
	clientsMu.Unlock()

	tableConfigsMu.Lock()
	tableConfigs = configs
	tableConfigsMu.Unlock()

	return nil
}

// RegisterTable 手动注册表配置
func RegisterTable(tableConfig *TableConfig) {
	tableConfigsMu.Lock()
	defer tableConfigsMu.Unlock()
	tableConfigs[tableConfig.TableName] = tableConfig
}

// getClient 获取指定实例的客户端（带缓存）。
// 缓存键包含 instanceName + endpoint + accessKey 指纹，避免相同实例名但不同凭证/endpoint 时返回错误客户端。
func getClient(instanceName, endpoint, accessKey, secretKey string) *tablestore.TableStoreClient {
	cacheKey := buildClientCacheKey(instanceName, endpoint, accessKey)

	clientsMu.RLock()
	client, exists := clients[cacheKey]
	clientsMu.RUnlock()

	if exists {
		return client
	}

	clientsMu.Lock()
	defer clientsMu.Unlock()

	if c, ok := clients[cacheKey]; ok {
		return c
	}

	client = tablestore.NewClient(
		endpoint,
		instanceName,
		accessKey,
		secretKey,
	)
	clients[cacheKey] = client
	return client
}

// buildClientCacheKey 构建客户端缓存键。
// 使用 instanceName + endpoint + accessKey 指纹，避免相同实例但不同凭证时复用错误客户端。
func buildClientCacheKey(instanceName, endpoint, accessKey string) string {
	return strings.Join([]string{instanceName, endpoint, accessKeyFingerprint(accessKey)}, "|")
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

	// 3. 获取对应实例的客户端（纯内存缓存，当前实现不会失败）
	client := getClient(tableConfig.InstanceName, endpoint, accessKey, secretKey)

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
		if err := addPrimaryKeyColumnByValue(pk, pkCol.ColumnName, val); err != nil {
			return nil, err
		}
	}
	return pk, nil
}

// preparePrimaryKeyAndEncodedNonPKColumns 从整行 map 解析主键，并生成「非主键列、*_json 已编码」的属性 map，供 Update 类路径复用。
// pkNames 可由调用方一次性构建并在批量循环中传入；为 nil 时在函数内调用 primaryKeyNameSet()。
func (op *SimpleTableOperator) preparePrimaryKeyAndEncodedNonPKColumns(row map[string]interface{}, pkNames map[string]bool) (*tablestore.PrimaryKey, map[string]interface{}, error) {
	if pkNames == nil {
		pkNames = op.primaryKeyNameSet()
	}
	pk, err := op.buildPrimaryKey(row)
	if err != nil {
		return nil, nil, err
	}
	attrs := shallowCopyMap(row)
	for col := range pkNames {
		delete(attrs, col)
	}
	if err := encodeJSONSuffixColumnsInMap(attrs); err != nil {
		return nil, nil, err
	}
	return pk, attrs, nil
}

// normalizePrimaryKeyValue 将常见的 Go 整型归一化为 int64，避免 OTS SDK 在构造主键时因 int 类型触发 invalid input（与属性列写入共用 normalizeIntegerToInt64）。
func normalizePrimaryKeyValue(v interface{}) interface{} {
	return normalizeIntegerToInt64(v)
}

// addPrimaryKeyColumnByValue 根据值类型选择正确的主键写入方式：
// - INF_MIN/INF_MAX 走 Min/Max API（用于范围查询边界）
// - 普通值走 AddPrimaryKeyColumn，并先做整型归一化
func addPrimaryKeyColumnByValue(pk *tablestore.PrimaryKey, name string, val interface{}) error {
	if pk == nil {
		return fmt.Errorf("primary key is nil")
	}
	switch v := val.(type) {
	case tablestore.PrimaryKeyOption:
		if v == tablestore.MIN {
			pk.AddPrimaryKeyColumnWithMinValue(name)
			return nil
		}
		if v == tablestore.MAX {
			pk.AddPrimaryKeyColumnWithMaxValue(name)
			return nil
		}
		return fmt.Errorf("unsupported primary key option for column %s: %v", name, v)
	default:
		pk.AddPrimaryKeyColumn(name, normalizePrimaryKeyValue(v))
		return nil
	}
}

// buildGetRowOptionsFromReadParams 将「列裁剪 + 最大版本」转换为内部统一的 GetRowOptions。
// maxVersion <= 0 时会在 row_api 层按 1 处理，与 TableStore 常见默认行为一致。
func buildGetRowOptionsFromReadParams(columnsToGet []string, maxVersion int32) *GetRowOptions {
	return &GetRowOptions{
		ColumnsToGet: columnsToGet,
		MaxVersion:   maxVersion,
	}
}

// GetRow 按主键读取一行。
// columnsToGet 非空时只拉取列出的属性列（主键列是否随列投影返回以 OTS 为准）；为空表示不限制列。
// maxVersion 为最大版本数；<=0 时按 1 处理。
// 主键字段须与 tables.yaml 中 primaryKeys 列名一致。
func (op *SimpleTableOperator) GetRow(primaryKeys map[string]interface{}, columnsToGet []string) (map[string]interface{}, error) {
	pk, err := op.buildPrimaryKey(primaryKeys)
	if err != nil {
		return nil, err
	}
	opts := buildGetRowOptionsFromReadParams(columnsToGet, 1)
	return op.tsClient().GetRowWithOptions(op.tableName, pk, opts)
}

// BatchGetRows 对当前表批量主键读取，每行 map 须包含完整主键列。
// columnsToGet 语义与 GetRow 一致；批量请求的 MaxVersion 固定为 1（与 SDK 约束及单行默认一致）。
func (op *SimpleTableOperator) BatchGetRows(rows []map[string]interface{}, columnsToGet []string) ([]BatchGetRowItem, error) {
	keys := make([]*tablestore.PrimaryKey, 0, len(rows))
	for _, r := range rows {
		pk, err := op.buildPrimaryKey(r)
		if err != nil {
			return nil, err
		}
		keys = append(keys, pk)
	}
	return op.tsClient().BatchGetRowSameTable(op.tableName, keys, columnsToGet)
}

// BatchWriteChanges 执行 BatchWriteRow：可同时提交 PutRowChange / UpdateRowChange / DeleteRowChange。
// 每个变更内的 TableName 须与该表一致（跨表批量时请使用 *Client.BatchWriteRowChanges）。
func (op *SimpleTableOperator) BatchWriteChanges(changes []tablestore.RowChange) error {
	return op.tsClient().BatchWriteRowChanges(changes)
}

// BatchWriteAction 批量写入操作
type BatchWriteAction struct {
	PutRows    []map[string]interface{}
	UpdateRows []map[string]interface{}
	DeleteRows []map[string]interface{}
	PutCond    *tablestore.RowCondition
	UpdateCond *tablestore.RowCondition
	DeleteCond *tablestore.RowCondition
}

// BatchWriteRows 批量写入操作。
// 支持 Put / Update / Delete 三种操作，支持条件控制；返回官方 SDK 的 BatchWriteRowResponse.TableToRowsResult（含每行结果、CU、错误详情等）。
// 不再返回ReqeustId, tablestoreV5中似乎已删除ReturnType, 不再返回PrimaryKey
func (op *SimpleTableOperator) BatchWriteRows(actions BatchWriteAction) ([]tablestore.RowResult, error) {
	total := len(actions.PutRows) + len(actions.UpdateRows) + len(actions.DeleteRows)
	// 预分配切片长度，避免对同一表键反复 append 触发多次扩容；最后一次性挂到请求上。
	rowChanges := make([]tablestore.RowChange, 0, total)
	pkNames := op.primaryKeyNameSet()

	putCond := defaultPutRowCondition(actions.PutCond)
	updateCond := defaultUpdateRowCondition(actions.UpdateCond)
	deleteCond := defaultDeleteRowCondition(actions.DeleteCond)

	for _, row := range actions.PutRows {
		putRowChange, err := op.assemblePutRowChangeFromMapWithPKNames(row, putCond, pkNames)
		if err != nil {
			return nil, err
		}
		rowChanges = append(rowChanges, putRowChange)
	}
	for _, row := range actions.UpdateRows {
		pk, attrs, err := op.preparePrimaryKeyAndEncodedNonPKColumns(row, pkNames)
		if err != nil {
			return nil, err
		}
		updateRowChange := &tablestore.UpdateRowChange{
			TableName:  op.tableName,
			PrimaryKey: pk,
			Condition:  updateCond,
		}
		for colName, val := range attrs {
			updateRowChange.PutColumn(colName, val)
		}
		if len(updateRowChange.Columns) == 0 {
			return nil, fmt.Errorf("batch update row: row must contain at least one non-primary-key column")
		}
		rowChanges = append(rowChanges, updateRowChange)
	}
	for _, row := range actions.DeleteRows {
		pk, err := op.buildPrimaryKey(row)
		if err != nil {
			return nil, err
		}
		deleteRowChange := &tablestore.DeleteRowChange{
			TableName:  op.tableName,
			PrimaryKey: pk,
			Condition:  deleteCond,
		}
		rowChanges = append(rowChanges, deleteRowChange)
	}
	batchRequest := &tablestore.BatchWriteRowRequest{
		RowChangesGroupByTable: map[string][]tablestore.RowChange{
			op.tableName: rowChanges,
		},
	}
	var resp *tablestore.BatchWriteRowResponse
	err := withRetry(func() error {
		var innerErr error
		resp, innerErr = op.client.BatchWriteRow(batchRequest)
		return innerErr
	})
	if err != nil {
		return nil, fmt.Errorf("batch write rows failed: %w", err)
	}
	if resp == nil {
		return nil, fmt.Errorf("batch write rows:响应为 nil")
	}
	rowResults, ok := resp.TableToRowsResult[op.tableName]
	if !ok {
		return nil, fmt.Errorf("batch write rows: 响应中缺少表 %q 的 TableToRowsResult", op.tableName)
	}
	if len(rowResults) == 0 {
		return nil, fmt.Errorf("batch write rows: 表 %q 的 RowResult 列表为空", op.tableName)
	}
	if len(rowResults) != total {
		return nil, fmt.Errorf("batch write rows: 期望 %d 条 RowResult，实际 %d 条", total, len(rowResults))
	}
	return rowResults, nil
}

// UpdateRowWithMutation 使用 UpdateMutation 更新一行（Put / 删列 / 自增 / 按版本删列）。
// func (op *SimpleTableOperator) UpdateRowWithMutation(primaryKeys map[string]interface{}, mut *UpdateMutation, cond *tablestore.RowCondition) error {
// 	pk, err := op.buildPrimaryKey(primaryKeys)
// 	if err != nil {
// 		return err
// 	}
// 	return op.tsClient().UpdateRowWithMutation(op.tableName, pk, mut, cond)
// }

// GetRangeWithPrimaryKeys 使用有序主键边界范围扫描，支持 ColumnToGet、反向、Limit 与 next_start_pk 分页；返回官方 SDK 的 GetRangeResponse。
// 将每行转为 map（含 *_json 解析）请对 resp.Rows 的元素调用 RowToMap。
func (op *SimpleTableOperator) GetRangeWithPrimaryKeys(startPK, endPK *tablestore.PrimaryKey, opts *GetRangeOptions) (*tablestore.ConsumedCapacityUnit, *tablestore.PrimaryKey, []map[string]interface{}, error) {
	resp, err := op.tsClient().GetRangeWithOptions(op.tableName, startPK, endPK, opts)
	if err != nil {
		return nil, nil, nil, err
	}
	rows := make([]map[string]interface{}, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		rows = append(rows, RowToMap(row))
	}
	return resp.ConsumedCapacityUnit, resp.NextStartPrimaryKey, rows, nil
}

// PutRow 写入一行（与 TableStore PutRow 语义一致）。data 须含全部主键列；*_json 列会以 JSON 字符串形式写入。
// cond 控制行存在性期望等（EXPECT_EXIST / EXPECT_NOT_EXIST / IGNORE）；传 nil 等价于 IGNORE（覆盖写入，与历史默认一致）。
// PutRowChange.ReturnType 固定为 RT_PK，以便服务端在自增主键等场景返回完整主键（与单行 PutRow 官方示例一致）。
func (op *SimpleTableOperator) PutRow(data map[string]interface{}, cond *tablestore.RowCondition) (*tablestore.PutRowResponse, error) {
	putRowChange, err := op.assemblePutRowChangeFromMap(data, cond)
	if err != nil {
		return nil, err
	}
	putRowChange.ReturnType = tablestore.ReturnType_RT_PK
	req := &tablestore.PutRowRequest{PutRowChange: putRowChange}
	var resp *tablestore.PutRowResponse
	err = withRetry(func() error {
		var innerErr error
		resp, innerErr = op.client.PutRow(req)
		return innerErr
	})
	if err != nil {
		return nil, fmt.Errorf("put row failed: %w", err)
	}
	return resp, nil
}

// UpdateData 表示单行 UpdateRow 的输入：主键与待 Put 列放在 RowData；可选整列删除与整型自增。
type UpdateData struct {
	// RowData 须包含 tables.yaml 中全部主键列；除主键外的键视为待 Put 的属性列（主键列不会作为 PutColumn 提交），编码规则与 PutRow 一致（含 *_json）。
	RowData map[string]interface{}
	// DeleteColumns 需要删除整列的列名列表，对应 SDK DeleteColumn。
	DeleteColumns []string
	// IncrementColumns 列名到增量的映射，对应 SDK IncrementColumn（仅适用于 INTEGER 类型属性列）。
	IncrementColumns map[string]int64
}

// UpdateRow 按主键对一行执行列变更：RowData 中的非主键列为 Put；DeleteColumns 删整列；IncrementColumns 自增。
// 三者至少其一非空，否则返回错误。cond 为 nil 时使用「行必须存在」（EXPECT_EXIST）；非 nil 时语义与 TableStore RowCondition 一致。
func (op *SimpleTableOperator) UpdateRow(data UpdateData, cond *tablestore.RowCondition) (*tablestore.UpdateRowResponse, error) {
	if data.RowData == nil {
		return nil, fmt.Errorf("update row: RowData 不能为 nil，须包含完整主键列")
	}

	effCond := cond
	if effCond == nil {
		effCond = &tablestore.RowCondition{
			RowExistenceExpectation: tablestore.RowExistenceExpectation_EXPECT_EXIST,
		}
	}

	pk, attrs, err := op.preparePrimaryKeyAndEncodedNonPKColumns(data.RowData, nil)
	if err != nil {
		return nil, err
	}

	updateRowChange := &tablestore.UpdateRowChange{
		TableName:  op.tableName,
		PrimaryKey: pk,
		Condition:  effCond,
	}
	for colName, val := range attrs {
		updateRowChange.PutColumn(colName, val)
	}
	for _, col := range data.DeleteColumns {
		if strings.TrimSpace(col) != "" {
			updateRowChange.DeleteColumn(col)
		}
	}
	for colName, delta := range data.IncrementColumns {
		if strings.TrimSpace(colName) != "" {
			updateRowChange.IncrementColumn(colName, delta)
		}
	}

	if len(updateRowChange.Columns) == 0 {
		return nil, fmt.Errorf("update row: 至少需提供一项列操作（RowData 中的非主键列、DeleteColumns 或 IncrementColumns）")
	}

	req := &tablestore.UpdateRowRequest{UpdateRowChange: updateRowChange}
	var resp *tablestore.UpdateRowResponse
	err = withRetry(func() error {
		var innerErr error
		resp, innerErr = op.client.UpdateRow(req)
		return innerErr
	})
	if err != nil {
		return nil, fmt.Errorf("update row failed: %w", err)
	}
	return resp, nil
}

// DeleteRow 按主键删除一行；返回 SDK 的 DeleteRowResponse（含 CU、RequestId 等），默认行存在性期望为 IGNORE。
func (op *SimpleTableOperator) DeleteRow(primaryKeys map[string]interface{}) (*tablestore.DeleteRowResponse, error) {
	pk, err := op.buildPrimaryKey(primaryKeys)
	if err != nil {
		return nil, err
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

	var resp *tablestore.DeleteRowResponse
	err = withRetry(func() error {
		var innerErr error
		resp, innerErr = op.client.DeleteRow(req)
		return innerErr
	})
	if err != nil {
		return nil, fmt.Errorf("delete row failed: %w", err)
	}
	return resp, nil
}

// BatchPutRows 批量 PutRow：每行 data 与 PutRow 相同（主键 + 属性列、*_json 编码）。
// condition 为 nil 时每一行均使用与 PutRow(cond==nil) 相同的默认 IGNORE；非 nil 时**所有行共用同一条件**（行存在性期望、列条件等与单行 PutRow 一致）。
// returnType 写入每一行 PutRowChange.ReturnType（全批量共用同一返回类型枚举）。
// 成功时直接返回官方 SDK 的 *BatchWriteRowResponse，TableToRowsResult 键为表名（当前实现仅包含本操作器绑定的表名），值为该行序对应的 []RowResult。
// 说明：部分 SDK 版本对 BatchWriteRow 响应中「返回行数据体」的解析较保守，RowResult 内 PrimaryKey/Columns 是否非空以所用 aliyun-tablestore-go-sdk 为准。
func (op *SimpleTableOperator) BatchPutRows(rows []map[string]interface{}, condition *tablestore.RowCondition, returnType tablestore.ReturnType) ([]tablestore.RowResult, error) {
	// 无行时与旧行为一致：不发起 RPC，返回空结果映射，避免调用方对 nil map 做写入。
	if len(rows) == 0 {
		return nil, nil
	}
	pkNames := op.primaryKeyNameSet()
	rowChanges := make([]tablestore.RowChange, 0, len(rows))
	for _, row := range rows {
		putRowChange, err := op.assemblePutRowChangeFromMapWithPKNames(row, condition, pkNames)
		if err != nil {
			return nil, err
		}
		putRowChange.ReturnType = returnType
		rowChanges = append(rowChanges, putRowChange)
	}
	batchRequest := &tablestore.BatchWriteRowRequest{
		RowChangesGroupByTable: map[string][]tablestore.RowChange{
			op.tableName: rowChanges,
		},
	}
	var resp *tablestore.BatchWriteRowResponse
	err := withRetry(func() error {
		var inner error
		resp, inner = op.client.BatchWriteRow(batchRequest)
		return inner
	})
	if err != nil {
		return nil, fmt.Errorf("batch put rows failed: %w", err)
	}
	// SDK 在成功路径下通常返回非 nil；若为 nil 则返回空结构，避免调用方解引用 panic。
	if resp == nil {
		return nil, nil
	}
	rowResults, ok := resp.TableToRowsResult[op.tableName]
	if !ok {
		return nil, fmt.Errorf("batch put rows: 响应中缺少表 %q 的 TableToRowsResult", op.tableName)
	}
	if len(rowResults) == 0 {
		return nil, fmt.Errorf("batch put rows: 表 %q 的 RowResult 列表为空", op.tableName)
	}
	if len(rowResults) != len(rows) {
		return nil, fmt.Errorf("batch put rows: 期望 %d 条 RowResult，实际 %d 条", len(rows), len(rowResults))
	}
	return rowResults, nil
}

// GetRange 范围查询（前闭后开等行为以 OTS 文档为准）。
// 会按 tables.yaml 中主键顺序构建边界，并支持将 simpleotsgo.INF_MIN/INF_MAX 作为范围主键值。
// 返回ConsumedCapacityUnit *tablestore.ConsumedCapacityUnit, NextStartPrimaryKey map[string]interface{}, Rows []map[string]interface{}, error
func (op *SimpleTableOperator) GetRange(startPK, endPK map[string]interface{}, direction tablestore.Direction, limit int32) (*tablestore.ConsumedCapacityUnit, map[string]interface{}, []map[string]interface{}, error) {
	if direction != tablestore.FORWARD && direction != tablestore.BACKWARD {
		return nil, nil, nil, fmt.Errorf("invalid range direction: %v", direction)
	}
	if startPK == nil || endPK == nil {
		return nil, nil, nil, fmt.Errorf("startPK and endPK are required")
	}
	// 按表配置中的主键顺序构建范围边界，避免直接遍历 map 导致联合主键顺序不稳定。
	startPrimaryKey, err := op.buildPrimaryKey(startPK)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid start primary key: %w", err)
	}
	endPrimaryKey, err := op.buildPrimaryKey(endPK)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("invalid end primary key: %w", err)
	}

	criteria := &tablestore.RangeRowQueryCriteria{
		TableName:       op.tableName,
		StartPrimaryKey: startPrimaryKey,
		EndPrimaryKey:   endPrimaryKey,
		Direction:       direction,
		Limit:           limit,
		MaxVersion:      1,
	}

	req := &tablestore.GetRangeRequest{
		RangeRowQueryCriteria: criteria,
	}

	var resp *tablestore.GetRangeResponse
	err = withRetry(func() error {
		var innerErr error
		resp, innerErr = op.client.GetRange(req)
		return innerErr
	})
	if err != nil {
		return nil, nil, nil, fmt.Errorf("get range failed: %w", err)
	}
	rows := make([]map[string]interface{}, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		rows = append(rows, RowToMap(row))
	}
	return resp.ConsumedCapacityUnit, PrimaryKeyToMap(resp.NextStartPrimaryKey), rows, nil
}

// GetTableName 获取表名
func (op *SimpleTableOperator) GetTableName() string {
	return op.tableName
}

// GetInstanceName 获取实例名
func (op *SimpleTableOperator) GetInstanceName() string {
	return op.config.InstanceName
}

func (op *SimpleTableOperator) GetRegionId() string {
	return op.config.RegionId
}

// GetAllRegisteredTables 获取所有已注册的表名
func GetAllRegisteredTables() []string {
	tableConfigsMu.RLock()
	defer tableConfigsMu.RUnlock()

	tables := make([]string, 0, len(tableConfigs))
	for tableName := range tableConfigs {
		tables = append(tables, tableName)
	}
	slices.Sort(tables)
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

// ResetForTesting 重置所有全局缓存与初始化状态，仅供测试使用。
// 生产代码不应调用此函数；它使同一进程内的不同测试用例可以独立初始化。
func ResetForTesting() {
	operatorsMu.Lock()
	operators = make(map[string]*SimpleTableOperator)
	operatorsMu.Unlock()

	clientsMu.Lock()
	clients = make(map[string]*tablestore.TableStoreClient)
	clientsMu.Unlock()

	tableConfigsMu.Lock()
	tableConfigs = make(map[string]*TableConfig)
	tableConfigsMu.Unlock()
}
