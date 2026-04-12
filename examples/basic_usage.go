package main

import (
	"fmt"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	sog "github.com/izut/simple-ots-go"
)

type TaskLogTable struct {
	TableName string
	Conn      sog.SimpleTableOperator
}

func NewTaskLogTable(tableName string) (*TaskLogTable, error) {
	conn, err := sog.Table(tableName)
	if err != nil {
		return nil, err
	}
	return &TaskLogTable{
		TableName: tableName,
		Conn:      *conn,
	}, nil
}

// PutRowExample 演示单行写入：第二个参数为行条件，EXPECT_NOT_EXIST 表示仅当行不存在时写入。
func (t *TaskLogTable) PutRowExample() {
	row := map[string]interface{}{
		"task_id":              "t_1001",
		"timestamp":            int64(1718181818),
		"log_type":             "info",
		"log_content":          "这是一条测试日志",
		"log_level":            "info",
		"log_source":           "test",
		"log_source_id":        "1234567890",
		"log_source_name":      "测试",
		"log_source_type":      "test",
		"log_source_type_name": "测试",
		"some_int":             1,
		"to_delete_column":     "更新时应该删除的列！！！",
	}
	// res, err := t.Conn.PutRow(row, sog.EXPECT_NOT_EXIST) // 此时消耗一个读CU, 一个写CU
	res, err := t.Conn.PutRow(row, sog.IGNORE) // 此时消耗一个读CU, 一个写CU, 相同，为什么？与上面速度应该是一致的,而且与return_type无关
	if err != nil {
		fmt.Printf("测试 PutRow 失败，错误: %+v\n", err)
		return
	}
	fmt.Println("测试 PutRow 成功，返回结果: ")
	fmt.Println("主键: ", res.PrimaryKey)
	fmt.Println("消耗(写/读): ", res.ConsumedCapacityUnit.Write, res.ConsumedCapacityUnit.Read)
	fmt.Println("请求ID: ", res.ResponseInfo.RequestId)

	// 读取该记录：nil 表示不限制列；maxVersion 传 0 也会按 1 处理（与 SDK 内部默认一致）。
	ReadAndPrintRow(map[string]interface{}{
		"task_id":   "t_1001",
		"timestamp": int64(1718181818),
	})
}

// UpdateRowExample 演示 UpdateData：RowData 内同时带主键与待 Put 的属性列；cond 传 nil 使用默认「行须已存在」。
func (t *TaskLogTable) UpdateRowExample() {
	row := map[string]interface{}{
		"task_id":     "t_1001",
		"timestamp":   int64(1718181818),
		"log_content": "这是一条测试日志（已更新）",
	}
	updateData := sog.UpdateData{
		RowData: row,
		DeleteColumns: []string{
			"to_delete_column",
		},
		IncrementColumns: map[string]int64{
			"some_int": 1,
		},
	}
	res, err := t.Conn.UpdateRow(updateData, nil)
	if err != nil {
		return
	}
	fmt.Println("更新成功", res)
	ReadAndPrintRow(map[string]interface{}{
		"task_id":   "t_1001",
		"timestamp": int64(1718181818),
	})
}

func (t *TaskLogTable) DeleteRowExample() {
	fmt.Print("\n-------------------------------------DeleteRow Example----------------------------------------------------Start\n删除测试开始\n")
	// DeleteRow 返回 DeleteRowResponse（CU 等）；此处用 `_` 忽略响应体。
	res, err := t.Conn.DeleteRow(map[string]interface{}{
		"task_id":   "t_1001",
		"timestamp": int64(1718181818),
	})
	if err != nil {
		fmt.Printf("测试 DeleteRow 失败，错误: %+v\n", err)
		return
	}
	fmt.Println("测试 DeleteRow 成功，返回结果: ")
	fmt.Println("消耗(写/读): ", res.ConsumedCapacityUnit.Write, res.ConsumedCapacityUnit.Read)
	fmt.Println("请求ID: ", res.ResponseInfo.RequestId)

	ReadAndPrintRow(map[string]interface{}{
		"task_id":   "t_1001",
		"timestamp": int64(1718181818),
	})

	fmt.Print("删除测试结束\n---------------------------DeleteRow Example--------------------------------------------------------------END\n\n")
}

func (t *TaskLogTable) GetRangeExample() {
	fmt.Print("\n-------------------------------------GetRange Example----------------------------------------------------Start\n获取范围测试开始\n")
	// 正向扫描下起始主键应整体「小于」结束主键：联合主键 (task_id, timestamp) 时两行均用 INF_MIN 作为起点，避免首列最小、次列最大落在该 task_id 分片末尾的歧义边界。
	spk := map[string]interface{}{
		"task_id":   sog.INF_MIN,
		"timestamp": sog.INF_MIN,
	}
	epk := map[string]interface{}{
		"task_id":   sog.INF_MAX,
		"timestamp": sog.INF_MAX,
	}
	getRangeRsult, err := t.Conn.GetRange(spk, epk, sog.FORWARD, 20)
	if err != nil {
		panic(err)
	}

	rows := getRangeRsult.Rows
	fmt.Printf("读取结果: %+v\n", rows)
	for _, row := range rows {
		PrintRow(sog.RowToMap(row))
	}
	nextStartPrimaryKey := getRangeRsult.NextStartPrimaryKey
	fmt.Printf("下一个主键[PrimaryKey]: %+v\n", nextStartPrimaryKey)
	fmt.Printf("下一个主键[map]: %+v\n", sog.PrimaryKeyToMap(nextStartPrimaryKey))
	cu := getRangeRsult.ConsumedCapacityUnit
	fmt.Printf("读消耗: %d，写消耗: %d\n", cu.Read, cu.Write)
	fmt.Printf("请求ID: %s\n", getRangeRsult.ResponseInfo.RequestId)

	fmt.Print("获取范围测试结束\n---------------------------GetRange Example--------------------------------------------------------------END\n\n")
}

func (t *TaskLogTable) BatchGetRowsExample() {
	fmt.Print("\n-------------------------------------BatchGetRows Example----------------------------------------------------Start\n批量读取测试开始\n")
	results, err := t.Conn.BatchGetRows([]map[string]interface{}{
		{
			"task_id":   "123",
			"timestamp": int64(1775810272538),
		},
		{
			"task_id":   "123",
			"timestamp": int64(1775810251869),
		},
	}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("批量读取结果: \n")
	for _, result := range results {

		if result.OK {
			fmt.Printf("第%d行成功，返回结果,", result.Index)
			PrintRow(result.Row)
			fmt.Printf("消耗(写/读): %d, %d\n", result.ConsumedCapacityUnit.Write, result.ConsumedCapacityUnit.Read)

		} else {
			fmt.Printf("第%d行错误: %+v\n", result.Index, result.Err)
		}
	}

	fmt.Print("批量读取测试结束\n---------------------------BatchGetRows Example--------------------------------------------------------------END\n\n")
}

func (t *TaskLogTable) BatchPutRowsExample() {
	fmt.Print("\n-------------------------------------BatchWriteRows Example----------------------------------------------------Start\n批量写入测试开始\n")
	put_rows := []map[string]interface{}{
		{
			"task_id":   "WF22:123",
			"timestamp": time.Now().UnixMilli(),
			"status":    "pending",
		},
		{
			"task_id":   "WF22:123",
			"timestamp": time.Now().UnixMilli(),
			"status":    "completed",
		},
	}
	batchWriteResult, err := t.Conn.BatchPutRows(put_rows, sog.IGNORE, tablestore.ReturnType_RT_NONE)
	if err != nil {
		panic(err)
	}
	fmt.Println("批量写入成功", batchWriteResult)
	fmt.Print("批量写入测试结束\n---------------------------BatchWriteRows Example--------------------------------------------------------------END\n\n")
}

func main() {
	// 业务方可通过环境变量指定 tables.yaml 路径。
	// 若未设置，SDK 默认读取当前工作目录下的 config/tables.yaml。
	// _ = os.Setenv("SIMPLEOTSGO_TABLES_PATH", "./config/tables.yaml")
	// 指定运行模式：development 使用公网，production 使用 VPC。
	// _ = os.Setenv("APP_ENV", "development")
	// 如果你希望完全手动控制 endpoint，可直接设置 TABLESTORE_ENDPOINT 覆盖自动拼接。
	// _ = os.Setenv("TABLESTORE_ENDPOINT", "https://your-instance.cn-hangzhou.ots.aliyuncs.com")

	// 直接按表名获取操作器：AK/SK 从环境变量读取；instanceName 与 regionId 仅从 tables.yaml 读取（勿用环境变量传地域）。
	// 通过环境变量指定 AK/SK,
	// set -a && source .env.example && set +a

	t, err := NewTaskLogTable("task_log")
	if err != nil {
		panic(err)
	}

	// 写入一条用户记录（PutRow，与 TableStore 命名一致）。第二个参数为行条件，nil 表示 IGNORE（覆盖写入）。
	t.PutRowExample()

	// 更新该记录：同一 map 内既含主键又含待更新属性列。

	// 更新该记录（UpdateData 仅 Put 列示例）。
	t.UpdateRowExample()

	// 删除该记录。
	t.DeleteRowExample()
	// 读取所有记录
	t.GetRangeExample()

	// 批量读取
	t.BatchGetRowsExample()
	// 测试批量写入
	t.BatchPutRowsExample()
}

func ReadAndPrintRow(pkmap map[string]interface{}) {
	taskTable, err := sog.Table("task_log")
	if err != nil {
		panic(err)
	}
	row, err := taskTable.GetRow(pkmap, nil, 0)
	if err != nil {
		panic(err)
	}
	if row == nil {
		fmt.Println("读取结果: 记录不存在, nil")
		return
	}

	fmt.Printf("读取结果:\n")
	PrintRow(row)
}

func PrintRow(row map[string]interface{}) {
	fmt.Println("\n行数据：\n--------------------------------")
	for k, v := range row {
		fmt.Printf("\t %-28s %v\n", k, v)
	}
	fmt.Println("=================================END")
}
