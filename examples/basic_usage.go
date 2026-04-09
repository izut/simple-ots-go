package main

import (
	"fmt"
	"os"

	"github.com/izut/simple-ots-go"
)

func main() {
	// 业务方可通过环境变量指定 tables.yaml 路径。
	// 若未设置，SDK 默认读取当前工作目录下的 config/tables.yaml。
	_ = os.Setenv("SIMPLEOTSGO_TABLES_PATH", "./config/tables.yaml")
	// 指定 TableStore 区域，SDK 会结合 tables.yaml 中的 instanceName 自动拼接 endpoint。
	_ = os.Setenv("TABLESTORE_AREA", "cn-hangzhou")
	// 指定运行模式：development 使用公网，production 使用 VPC。
	_ = os.Setenv("SIMPLEOTSGO_RUN_MODE", "development")
	// 如果你希望完全手动控制 endpoint，可直接设置 TABLESTORE_ENDPOINT 覆盖自动拼接。
	// _ = os.Setenv("TABLESTORE_ENDPOINT", "https://your-instance.cn-hangzhou.ots.aliyuncs.com")

	// 创建最简入口：只需要 AccessKey/SecretKey。
	// SDK 会自动加载 tables.yaml，并根据模式自动选择公网/VPC endpoint。
	operator, err := simpleotsgo.New("your-access-key", "your-secret-key")
	if err != nil {
		panic(err)
	}

	// 根据表名获取操作器，表结构和 instanceName 从 tables.yaml 自动读取。
	userTable, err := operator.Table("user")
	if err != nil {
		panic(err)
	}

	// 写入一条用户记录（PutRow，与 TableStore 命名一致）。
	err = userTable.PutRow(map[string]interface{}{
		"uid":       "u_1001",
		"user_name": "张三",
		"email":     "zhangsan@example.com",
		"status":    int64(1),
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("写入成功")

	// 读取该记录；第二个参数为 nil 表示默认列与版本策略。
	row, err := userTable.GetRow(map[string]interface{}{
		"uid": "u_1001",
	}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("读取结果: %+v\n", row)

	// 更新该记录。
	err = userTable.UpdateRow(
		map[string]interface{}{"uid": "u_1001"},
		map[string]interface{}{"user_name": "张三（已更新）"},
	)
	if err != nil {
		panic(err)
	}
	fmt.Println("更新成功")

	// 删除该记录。
	err = userTable.DeleteRow(map[string]interface{}{
		"uid": "u_1001",
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("删除成功")
}
