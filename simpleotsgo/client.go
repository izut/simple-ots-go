package simpleotsgo

import (
	"fmt"
	"os"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
)

// Client 是对官方 TableStore 客户端的轻量封装。
// 该类型适合“已知实例名”的直接调用场景，不依赖 tables.yaml。
type Client struct {
	client *tablestore.TableStoreClient
}

// Config 定义客户端初始化参数。
// 若字段为空，会尝试从同名环境变量中读取默认值。
type Config struct {
	Endpoint        string
	InstanceName    string
	AccessKeyID     string
	AccessKeySecret string
}

// Option 是函数式配置项，用于覆盖 Config 默认值。
type Option func(*Config)

// WithEndpoint 设置 TableStore 访问地址。
func WithEndpoint(endpoint string) Option {
	return func(c *Config) {
		c.Endpoint = endpoint
	}
}

// WithInstance 设置实例名称。
func WithInstance(instance string) Option {
	return func(c *Config) {
		c.InstanceName = instance
	}
}

// WithAccessKey 设置访问密钥 ID。
func WithAccessKey(accessKey string) Option {
	return func(c *Config) {
		c.AccessKeyID = accessKey
	}
}

// WithSecretKey 设置访问密钥 Secret。
func WithSecretKey(secretKey string) Option {
	return func(c *Config) {
		c.AccessKeySecret = secretKey
	}
}

// NewClient 创建基础客户端。
// 使用策略：先读取环境变量，再应用函数式参数覆盖，最后进行必填校验。
func NewClient(opts ...Option) (*Client, error) {
	// 先从环境变量读取默认配置，便于在 CI 或容器环境直接使用。
	config := &Config{
		Endpoint:        os.Getenv("TABLESTORE_ENDPOINT"),
		InstanceName:    os.Getenv("TABLESTORE_INSTANCE"),
		AccessKeyID:     os.Getenv("TABLESTORE_ACCESS_KEY"),
		AccessKeySecret: os.Getenv("TABLESTORE_SECRET_KEY"),
	}

	// 再应用调用方传入的显式配置，以显式参数优先。
	for _, opt := range opts {
		opt(config)
	}

	// 严格校验关键参数，尽早失败以减少运行期定位成本。
	if config.Endpoint == "" {
		return nil, fmt.Errorf("endpoint is required")
	}
	if config.InstanceName == "" {
		return nil, fmt.Errorf("instance name is required")
	}
	if config.AccessKeyID == "" {
		return nil, fmt.Errorf("access key is required")
	}
	if config.AccessKeySecret == "" {
		return nil, fmt.Errorf("secret key is required")
	}

	// 创建官方 SDK 客户端实例。
	client := tablestore.NewClient(
		config.Endpoint,
		config.InstanceName,
		config.AccessKeyID,
		config.AccessKeySecret,
	)

	return &Client{client: client}, nil
}

// GetClient 返回底层官方客户端，便于调用方按需使用高级能力。
func (c *Client) GetClient() *tablestore.TableStoreClient {
	return c.client
}

// PutRow 写入（或覆盖）一行数据。
// 注意：该方法将 map 中第一组键值视为主键，仅用于简单场景；列名以 _json 结尾的属性在写入前会将 map/slice 等序列化为字符串。
func (c *Client) PutRow(table string, data map[string]interface{}) error {
	rowForOTS := shallowCopyMap(data)
	if err := encodeJSONSuffixColumnsInMap(rowForOTS); err != nil {
		return err
	}
	// 构建主键。当前实现采用“首字段即主键”的简化策略。
	primaryKey := &tablestore.PrimaryKey{}
	for k, v := range rowForOTS {
		// 该策略并不适用于联合主键，仅建议在演示或快速脚本场景使用。
		primaryKey.AddPrimaryKeyColumn(k, v)
		break
	}

	// 构建 PutRow 请求。
	putRowChange := &tablestore.PutRowChange{
		TableName:  table,
		PrimaryKey: primaryKey,
		Condition: &tablestore.RowCondition{
			RowExistenceExpectation: tablestore.RowExistenceExpectation_IGNORE,
		},
	}

	// 添加属性列（跳过上方已作为主键使用的首字段）。
	first := true
	for k, v := range rowForOTS {
		if first {
			first = false
			continue // Skip primary key
		}
		putRowChange.AddColumn(k, v)
	}

	// 组装请求对象。
	req := &tablestore.PutRowRequest{
		PutRowChange: putRowChange,
	}

	// 执行请求，并通过统一重试机制处理瞬态失败。
	var err error
	err = withRetry(func() error {
		_, innerErr := c.client.PutRow(req)
		return innerErr
	})
	if err != nil {
		return fmt.Errorf("put row failed: %w", err)
	}

	return nil
}

// GetRow 按主键读取单行数据。
func (c *Client) GetRow(table string, primaryKey map[string]interface{}) (map[string]interface{}, error) {
	// 构建主键。
	pk := &tablestore.PrimaryKey{}
	for k, v := range primaryKey {
		pk.AddPrimaryKeyColumn(k, v)
	}

	// 构建查询条件。
	criteria := &tablestore.SingleRowQueryCriteria{
		TableName:  table,
		PrimaryKey: pk,
		MaxVersion: 1,
	}

	// 组装请求对象。
	req := &tablestore.GetRowRequest{
		SingleRowQueryCriteria: criteria,
	}

	// 执行请求（含重试）。
	var resp *tablestore.GetRowResponse
	err := withRetry(func() error {
		var innerErr error
		resp, innerErr = c.client.GetRow(req)
		return innerErr
	})
	if err != nil {
		return nil, fmt.Errorf("get row failed: %w", err)
	}

	// 将主键与属性列写入 map，并对 *_json 列做反序列化（与 Operator 行为一致）。
	result := make(map[string]interface{})
	for _, col := range resp.PrimaryKey.PrimaryKeys {
		result[col.ColumnName] = col.Value
	}
	for _, col := range resp.Columns {
		result[col.ColumnName] = col.Value
	}
	decodeJSONSuffixColumns(result)
	return result, nil
}

// UpdateRow 更新一行数据中的部分列。
func (c *Client) UpdateRow(table string, primaryKey map[string]interface{}, updates map[string]interface{}) error {
	// 构建主键。
	pk := &tablestore.PrimaryKey{}
	for k, v := range primaryKey {
		pk.AddPrimaryKeyColumn(k, v)
	}

	// 构建 UpdateRow 请求。
	updateRowChange := &tablestore.UpdateRowChange{
		TableName:  table,
		PrimaryKey: pk,
		Condition: &tablestore.RowCondition{
			RowExistenceExpectation: tablestore.RowExistenceExpectation_EXPECT_EXIST,
		},
	}

	updatesForOTS := shallowCopyMap(updates)
	if err := encodeJSONSuffixColumnsInMap(updatesForOTS); err != nil {
		return err
	}
	for k, v := range updatesForOTS {
		updateRowChange.PutColumn(k, v)
	}

	// 组装请求对象。
	req := &tablestore.UpdateRowRequest{
		UpdateRowChange: updateRowChange,
	}

	// 执行请求（含重试）。
	var err error
	err = withRetry(func() error {
		_, innerErr := c.client.UpdateRow(req)
		return innerErr
	})
	if err != nil {
		return fmt.Errorf("update row failed: %w", err)
	}

	return nil
}

// DeleteRow 按主键删除一行数据。
func (c *Client) DeleteRow(table string, primaryKey map[string]interface{}) error {
	// 构建主键。
	pk := &tablestore.PrimaryKey{}
	for k, v := range primaryKey {
		pk.AddPrimaryKeyColumn(k, v)
	}

	// 构建 DeleteRow 请求。
	deleteRowChange := &tablestore.DeleteRowChange{
		TableName:  table,
		PrimaryKey: pk,
		Condition: &tablestore.RowCondition{
			RowExistenceExpectation: tablestore.RowExistenceExpectation_IGNORE,
		},
	}

	// 组装请求对象。
	req := &tablestore.DeleteRowRequest{
		DeleteRowChange: deleteRowChange,
	}

	// 执行请求（含重试）。
	var err error
	err = withRetry(func() error {
		_, innerErr := c.client.DeleteRow(req)
		return innerErr
	})
	if err != nil {
		return fmt.Errorf("delete row failed: %w", err)
	}

	return nil
}

// BatchPutRow 批量写入多行数据。
// 注意：该方法同样采用“每行首字段为主键”的简化约定。
func (c *Client) BatchPutRow(table string, rows []map[string]interface{}) error {
	batchWriteRowRequest := &tablestore.BatchWriteRowRequest{
		RowChangesGroupByTable: make(map[string][]tablestore.RowChange),
	}

	for _, row := range rows {
		rowForOTS := shallowCopyMap(row)
		if err := encodeJSONSuffixColumnsInMap(rowForOTS); err != nil {
			return err
		}
		// 构建主键。
		primaryKey := &tablestore.PrimaryKey{}
		for k, v := range rowForOTS {
			primaryKey.AddPrimaryKeyColumn(k, v)
			break
		}

		// 构建单行写入变更。
		putRowChange := &tablestore.PutRowChange{
			TableName:  table,
			PrimaryKey: primaryKey,
			Condition: &tablestore.RowCondition{
				RowExistenceExpectation: tablestore.RowExistenceExpectation_IGNORE,
			},
		}

		// 添加属性列。
		first := true
		for k, v := range rowForOTS {
			if first {
				first = false
				continue // Skip primary key
			}
			putRowChange.AddColumn(k, v)
		}

		batchWriteRowRequest.RowChangesGroupByTable[table] = append(
			batchWriteRowRequest.RowChangesGroupByTable[table],
			putRowChange,
		)
	}

	// 执行批量请求（含重试）。
	var err error
	err = withRetry(func() error {
		_, innerErr := c.client.BatchWriteRow(batchWriteRowRequest)
		return innerErr
	})
	if err != nil {
		return fmt.Errorf("batch put row failed: %w", err)
	}

	return nil
}

// GetRange 执行范围查询。
// 该方法使用前闭后开区间，具体边界行为以 TableStore 官方语义为准。
func (c *Client) GetRange(table string, startPK, endPK map[string]interface{}, limit int32) ([]map[string]interface{}, error) {
	// 构建起始主键。
	startPrimaryKey := &tablestore.PrimaryKey{}
	for k, v := range startPK {
		startPrimaryKey.AddPrimaryKeyColumn(k, v)
	}

	// 构建结束主键。
	endPrimaryKey := &tablestore.PrimaryKey{}
	for k, v := range endPK {
		endPrimaryKey.AddPrimaryKeyColumn(k, v)
	}

	// 构建范围查询条件。
	criteria := &tablestore.RangeRowQueryCriteria{
		TableName:       table,
		StartPrimaryKey: startPrimaryKey,
		EndPrimaryKey:   endPrimaryKey,
		Direction:       tablestore.FORWARD,
		Limit:           limit,
		MaxVersion:      1,
	}

	// 组装请求对象。
	req := &tablestore.GetRangeRequest{
		RangeRowQueryCriteria: criteria,
	}

	// 执行查询请求（含重试）。
	var resp *tablestore.GetRangeResponse
	err := withRetry(func() error {
		var innerErr error
		resp, innerErr = c.client.GetRange(req)
		return innerErr
	})
	if err != nil {
		return nil, fmt.Errorf("get range failed: %w", err)
	}

	// 转换查询结果，统一返回 map 列表格式（含主键列与 *_json 反序列化）。
	results := make([]map[string]interface{}, 0, len(resp.Rows))
	for _, row := range resp.Rows {
		result := make(map[string]interface{})
		if row.PrimaryKey != nil {
			for _, pkCol := range row.PrimaryKey.PrimaryKeys {
				result[pkCol.ColumnName] = pkCol.Value
			}
		}
		for _, col := range row.Columns {
			result[col.ColumnName] = col.Value
		}
		decodeJSONSuffixColumns(result)
		results = append(results, result)
	}

	return results, nil
}
