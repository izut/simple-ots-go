package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	sog "github.com/izut/simple-ots-go"
)

// loadDotEnv 从 .env 读取 KEY=VALUE 到进程环境变量。
// 仅在该 key 当前未设置时写入，避免覆盖外部已注入的敏感配置。
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			continue
		}
		k := strings.TrimSpace(kv[0])
		v := strings.TrimSpace(kv[1])
		if k == "" || os.Getenv(k) != "" {
			continue
		}
		_ = os.Setenv(k, v)
	}
}

func main() {
	// fmt.Println("TestMain")
	// // 本地调试时自动加载 .env（若不存在则忽略）。
	// loadDotEnv(".env")
	// 地域请在 config/tables.yaml 各表下配置 regionId。
	// // 开发模式走公网，生产模式走 VPC。
	// _ = os.Setenv("APP_ENV", "development")
	// // 可选：指定 tables.yaml，默认 ./config/tables.yaml。
	// _ = os.Setenv("SIMPLEOTSGO_TABLES_PATH", "./config/tables.yaml")
	taskLog, err := sog.Table("task_log")
	if err != nil {
		panic(err)
	}

	// if _, err := taskLog.PutRow(map[string]interface{}{
	// 	"task_id":        "123",
	// 	"timestamp":      time.Now().UnixMilli(), // task_log 的第二主键为 INTEGER，这里必须传 int64
	// 	"log_type":       "demo",
	// 	"log_level":      "INFO",
	// 	"message":        "test",
	// 	"meta_data_json": map[string]interface{}{"result": "success"},
	// 	"created_at":     time.Now().UnixMilli(),
	// 	"author":         "izut",
	// }, nil); err != nil {
	// 	panic(err)
	// }

	// fmt.Println(taskLog)

	// GetRange 返回 (CU, 下一页起始主键 map, 已解码行 map 列表, error)。
	_, _, rows, err := taskLog.GetRange(map[string]interface{}{
		"task_id":   "123",
		"timestamp": sog.INF_MIN,
	}, map[string]interface{}{
		"task_id":   "123",
		"timestamp": sog.INF_MAX,
	}, sog.FORWARD, 10)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		fmt.Printf("row: %+v\n", row)
	}

}
