package simpleotsgo

import (
	"fmt"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
)

// PKEntry 表示主键的一列（列名 + 值），按参数顺序组成有序主键，供联合主键或 BatchGet 使用。
type PKEntry struct {
	Name  string
	Value interface{}
}

// NewPrimaryKey 按给定顺序构造 TableStore PrimaryKey（联合主键时请按表定义顺序传入各 PKEntry）。
func NewPrimaryKey(entries ...PKEntry) *tablestore.PrimaryKey {
	pk := &tablestore.PrimaryKey{}
	for _, e := range entries {
		pk.AddPrimaryKeyColumn(e.Name, e.Value)
	}
	return pk
}

// PrimaryKeyToMap 将 SDK 的 PrimaryKey 转为 map，便于日志或作为 loosely-typed 参数传递。
// 注意：map 遍历无序，不能用于再次构造范围扫描边界；续翻分页请直接使用返回的 *tablestore.PrimaryKey。
func PrimaryKeyToMap(pk *tablestore.PrimaryKey) map[string]interface{} {
	if pk == nil {
		return nil
	}
	m := make(map[string]interface{}, len(pk.PrimaryKeys))
	for _, c := range pk.PrimaryKeys {
		m[c.ColumnName] = c.Value
	}
	return m
}

// GetRowOptions 控制 GetRow 的列投影与版本。
type GetRowOptions struct {
	// ColumnsToGet 非空时只拉取列出的属性列（及主键列，行为以 OTS 为准）；为空表示不限制列。
	ColumnsToGet []string
	// MaxVersion 最大版本数，<=0 时由 SDK 封装层归为 1。
	MaxVersion int32
}

func applyMaxVersion(mv int32) int32 {
	if mv <= 0 {
		return 1
	}
	return mv
}

func applySingleRowCriteriaOpts(c *tablestore.SingleRowQueryCriteria, opts *GetRowOptions) {
	if opts == nil {
		c.MaxVersion = 1
		return
	}
	c.MaxVersion = applyMaxVersion(opts.MaxVersion)
	for _, col := range opts.ColumnsToGet {
		if col != "" {
			c.AddColumnToGet(col)
		}
	}
}

func applyRangeCriteriaOpts(c *tablestore.RangeRowQueryCriteria, opts *GetRangeOptions) {
	c.Direction = tablestore.FORWARD
	c.MaxVersion = 1
	if opts == nil {
		return
	}
	for _, col := range opts.ColumnsToGet {
		if col != "" {
			c.AddColumnToGet(col)
		}
	}
	if opts.Direction == tablestore.BACKWARD {
		c.Direction = tablestore.BACKWARD
	}
	if opts.Limit > 0 {
		c.Limit = opts.Limit
	}
	c.MaxVersion = applyMaxVersion(opts.MaxVersion)
	c.ReturnSpecifiedPkOnly = opts.ReturnSpecifiedPkOnly
}

// GetRowWithOptions 按主键读单行，支持 ColumnsToGet 列裁剪与 MaxVersion。
func (c *Client) GetRowWithOptions(table string, pk *tablestore.PrimaryKey, opts *GetRowOptions) (map[string]interface{}, error) {
	if pk == nil {
		return nil, fmt.Errorf("primary key is nil")
	}
	criteria := &tablestore.SingleRowQueryCriteria{
		TableName:  table,
		PrimaryKey: pk,
	}
	applySingleRowCriteriaOpts(criteria, opts)
	req := &tablestore.GetRowRequest{SingleRowQueryCriteria: criteria}
	var resp *tablestore.GetRowResponse
	err := withRetry(func() error {
		var inner error
		resp, inner = c.client.GetRow(req)
		return inner
	})
	if err != nil {
		return nil, fmt.Errorf("get row failed: %w", err)
	}
	return getRowResponseToMap(resp), nil
}

func getRowResponseToMap(resp *tablestore.GetRowResponse) map[string]interface{} {
	m := make(map[string]interface{})
	if resp == nil {
		return m
	}
	for _, pkCol := range resp.PrimaryKey.PrimaryKeys {
		m[pkCol.ColumnName] = pkCol.Value
	}
	for _, col := range resp.Columns {
		m[col.ColumnName] = col.Value
	}
	// 将 *_json 列从 STRING 反序列化为 map/slice 等（TableStore 无 JSON 类型）。
	decodeJSONSuffixColumns(m)
	return m
}

// tableStoreRowToDecodedMap 将 OTS 返回的一行（Row）统一转为 map 并完成 *_json 列解析，供 GetRange 与 rowToMap 共用。
func tableStoreRowToDecodedMap(row *tablestore.Row) map[string]interface{} {
	m := make(map[string]interface{})
	if row != nil && row.PrimaryKey != nil {
		for _, pkCol := range row.PrimaryKey.PrimaryKeys {
			m[pkCol.ColumnName] = pkCol.Value
		}
	}
	if row != nil {
		for _, col := range row.Columns {
			m[col.ColumnName] = col.Value
		}
	}
	decodeJSONSuffixColumns(m)
	return m
}

func rowToMap(row *tablestore.Row) map[string]interface{} {
	return tableStoreRowToDecodedMap(row)
}

// BatchGetRowItem 表示 BatchGetRow 中单行结果（含行级错误）。
type BatchGetRowItem struct {
	Index int           // 与请求中主键顺序一致
	OK    bool          // 是否成功
	Err   error         // 失败时 OTS 返回的语义化错误
	Row   map[string]interface{} // 成功且行存在时的属性 + 主键列
}

func rowResultToMap(r *tablestore.RowResult) map[string]interface{} {
	m := make(map[string]interface{})
	for _, pkCol := range r.PrimaryKey.PrimaryKeys {
		m[pkCol.ColumnName] = pkCol.Value
	}
	for _, col := range r.Columns {
		m[col.ColumnName] = col.Value
	}
	decodeJSONSuffixColumns(m)
	return m
}

// BatchGetRowSameTable 对单张表批量主键读取（共享 ColumnsToGet / MaxVersion）。
func (c *Client) BatchGetRowSameTable(table string, keys []*tablestore.PrimaryKey, opts *GetRowOptions) ([]BatchGetRowItem, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	mv := int32(1)
	var cols []string
	if opts != nil {
		mv = applyMaxVersion(opts.MaxVersion)
		cols = append(cols, opts.ColumnsToGet...)
	}
	mrc := &tablestore.MultiRowQueryCriteria{
		TableName:  table,
		MaxVersion: int(mv),
		PrimaryKey: keys,
	}
	for _, col := range cols {
		if col != "" {
			mrc.AddColumnToGet(col)
		}
	}
	req := &tablestore.BatchGetRowRequest{
		MultiRowQueryCriteria: []*tablestore.MultiRowQueryCriteria{mrc},
	}
	var resp *tablestore.BatchGetRowResponse
	err := withRetry(func() error {
		var inner error
		resp, inner = c.client.BatchGetRow(req)
		return inner
	})
	if err != nil {
		return nil, fmt.Errorf("batch get row failed: %w", err)
	}
	rows, ok := resp.TableToRowsResult[table]
	if !ok {
		return nil, fmt.Errorf("batch get row: missing results for table %q", table)
	}
	out := make([]BatchGetRowItem, len(rows))
	for i := range rows {
		rr := rows[i]
		item := BatchGetRowItem{Index: i, OK: rr.IsSucceed}
		if !rr.IsSucceed {
			item.Err = fmt.Errorf("%s: %s", rr.Error.Code, rr.Error.Message)
			out[i] = item
			continue
		}
		item.Row = rowResultToMap(&rr)
		out[i] = item
	}
	return out, nil
}

// BatchWriteRowChanges 执行 BatchWriteRow：可同时包含 Put / Update / Delete 等 RowChange。
// 注意：OTS 对单次 Batch 行数有限制；超大批量请在业务层分片。
func (c *Client) BatchWriteRowChanges(changes []tablestore.RowChange) error {
	if len(changes) == 0 {
		return nil
	}
	req := &tablestore.BatchWriteRowRequest{
		RowChangesGroupByTable: make(map[string][]tablestore.RowChange),
	}
	for _, ch := range changes {
		tn := ch.GetTableName()
		req.RowChangesGroupByTable[tn] = append(req.RowChangesGroupByTable[tn], ch)
	}
	var err error
	err = withRetry(func() error {
		_, inner := c.client.BatchWriteRow(req)
		return inner
	})
	if err != nil {
		return fmt.Errorf("batch write row failed: %w", err)
	}
	return nil
}

// ColumnVersionDelete 表示删除某一列的指定版本。
type ColumnVersionDelete struct {
	Name      string
	Timestamp int64
}

// UpdateMutation 描述 UpdateRow 的多种列变更：Put、删除整列、自增、按版本删除。
type UpdateMutation struct {
	Put          map[string]interface{}
	DeleteAll    []string
	Increment    map[string]int64
	DeleteOneVer []ColumnVersionDelete
}

// BuildUpdateRowChange 由 UpdateMutation 生成 UpdateRowChange，cond 为 nil 时使用「行必须存在」。
func BuildUpdateRowChange(table string, pk *tablestore.PrimaryKey, mut *UpdateMutation, cond *tablestore.RowCondition) (*tablestore.UpdateRowChange, error) {
	if mut == nil {
		return nil, fmt.Errorf("mutation is nil")
	}
	if pk == nil {
		return nil, fmt.Errorf("primary key is nil")
	}
	if cond == nil {
		cond = &tablestore.RowCondition{
			RowExistenceExpectation: tablestore.RowExistenceExpectation_EXPECT_EXIST,
		}
	}
	ch := &tablestore.UpdateRowChange{
		TableName:  table,
		PrimaryKey: pk,
		Condition:  cond,
	}
	for k, v := range mut.Put {
		vv, encErr := encodeJSONCellValueForTableStore(k, v)
		if encErr != nil {
			return nil, encErr
		}
		ch.PutColumn(k, vv)
	}
	for _, col := range mut.DeleteAll {
		if col != "" {
			ch.DeleteColumn(col)
		}
	}
	for k, delta := range mut.Increment {
		ch.IncrementColumn(k, delta)
	}
	for _, d := range mut.DeleteOneVer {
		ch.DeleteColumnWithTimestamp(d.Name, d.Timestamp)
	}
	if len(ch.Columns) == 0 {
		return nil, fmt.Errorf("mutation has no column operations")
	}
	return ch, nil
}

// UpdateRowWithMutation 执行带多种列操作的 UpdateRow。
func (c *Client) UpdateRowWithMutation(table string, pk *tablestore.PrimaryKey, mut *UpdateMutation, cond *tablestore.RowCondition) error {
	ch, err := BuildUpdateRowChange(table, pk, mut, cond)
	if err != nil {
		return err
	}
	req := &tablestore.UpdateRowRequest{UpdateRowChange: ch}
	return withRetry(func() error {
		_, inner := c.client.UpdateRow(req)
		return inner
	})
}

// GetRangeOptions 控制 GetRange：列投影、正反向、Limit、版本数、主键投影选项。
type GetRangeOptions struct {
	ColumnsToGet []string
	// Direction 使用 tablestore.FORWARD（默认）或 tablestore.BACKWARD 反向扫描。
	Direction tablestore.Direction
	Limit     int32
	MaxVersion int32
	// ReturnSpecifiedPkOnly 为 true 且 ColumnsToGet 未包含全部主键列时，仅返回列裁剪中的主键子集（见 OTS 文档）。
	ReturnSpecifiedPkOnly bool
}

// GetRangePage 表示一页 GetRange 结果。
// 分页：若 NextStartPrimaryKey 非 nil，则以之为新的 StartPrimaryKey、保持相同 EndPrimaryKey 与 Direction 再次调用 GetRangeWithOptions，直至 NextStartPrimaryKey 为 nil。
type GetRangePage struct {
	Rows                 []map[string]interface{}
	NextStartPrimaryKey  *tablestore.PrimaryKey
}

// GetRangeWithOptions 范围查询：支持 ColumnToGet、反向扫描、Limit；通过 NextStartPrimaryKey 分页。
func (c *Client) GetRangeWithOptions(table string, startPK, endPK *tablestore.PrimaryKey, opts *GetRangeOptions) (*GetRangePage, error) {
	if startPK == nil || endPK == nil {
		return nil, fmt.Errorf("start or end primary key is nil")
	}
	criteria := &tablestore.RangeRowQueryCriteria{
		TableName:       table,
		StartPrimaryKey: startPK,
		EndPrimaryKey:   endPK,
	}
	applyRangeCriteriaOpts(criteria, opts)
	req := &tablestore.GetRangeRequest{RangeRowQueryCriteria: criteria}
	var resp *tablestore.GetRangeResponse
	err := withRetry(func() error {
		var inner error
		resp, inner = c.client.GetRange(req)
		return inner
	})
	if err != nil {
		return nil, fmt.Errorf("get range failed: %w", err)
	}
	page := &GetRangePage{
		Rows:                make([]map[string]interface{}, 0, len(resp.Rows)),
		NextStartPrimaryKey: resp.NextStartPrimaryKey,
	}
	for _, row := range resp.Rows {
		page.Rows = append(page.Rows, rowToMap(row))
	}
	return page, nil
}
