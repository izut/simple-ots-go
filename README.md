# SimpleOTSGo

简单好用的 Go TableStore SDK。  
你只需要 `AccessKey + SecretKey`，然后按表名获取操作器即可 CRUD。

## 安装

```bash
go get github.com/izut/simple-ots-go
```

## 1 分钟上手（推荐）

```go
package main

import (
	"fmt"
	"os"

	"github.com/izut/simple-ots-go"
)

func main() {
	// 指定区域（如 cn-hangzhou）。
	_ = os.Setenv("TABLESTORE_AREA", "cn-hangzhou")
	// 开发模式走公网，生产模式走 VPC。
	_ = os.Setenv("SIMPLEOTSGO_RUN_MODE", "development")
	// 可选：指定 tables.yaml，默认 ./config/tables.yaml。
	_ = os.Setenv("SIMPLEOTSGO_TABLES_PATH", "./config/tables.yaml")

	op, err := simpleotsgo.New("your-access-key", "your-secret-key")
	if err != nil {
		panic(err)
	}

	userTable, err := op.Table("user")
	if err != nil {
		panic(err)
	}

	// Create
	err = userTable.PutRow(map[string]interface{}{
		"uid":       "user_00001",
		"user_name": "张三",
		"email":     "zhangsan@example.com",
		"status":    int64(1),
	})
	if err != nil {
		panic(err)
	}

	// Read
	row, err := userTable.GetRow(map[string]interface{}{"uid": "user_00001"}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("row=%+v\n", row)

	// Update
	err = userTable.UpdateRow(
		map[string]interface{}{"uid": "user_00001"},
		map[string]interface{}{"user_name": "张三-已更新"},
	)
	if err != nil {
		panic(err)
	}

	// Delete
	err = userTable.DeleteRow(map[string]interface{}{"uid": "user_00001"})
	if err != nil {
		panic(err)
	}
}
```

## 关键规则（必看）

- `instanceName` 来自 `tables.yaml` 中对应表配置
- `TABLESTORE_AREA` 决定区域（如 `cn-hangzhou`）
- `SIMPLEOTSGO_RUN_MODE=development` 使用公网 endpoint
- `SIMPLEOTSGO_RUN_MODE=production` 使用 VPC endpoint
- `GO_ENV=production` 与 `SIMPLEOTSGO_RUN_MODE=production` 等价参与判定（与常见 Go 项目习惯一致）
- 显式设置 `TABLESTORE_ENDPOINT` 时，优先使用显式值

## 同步表结构（CLI）

支持双向同步，**必须显式指定一种方向**：

- `-push`：`tables.yaml -> TableStore`（创建/重建表）
- `-pull`：`TableStore -> tables.yaml`（回写远程表结构）

```bash
export TABLESTORE_AREA=cn-hangzhou
export SIMPLEOTSGO_RUN_MODE=development   # 生产改 production 或 export GO_ENV=production
export TABLESTORE_ACCESS_KEY_ID=...
export TABLESTORE_ACCESS_KEY_SECRET=...
# 或使用：TABLESTORE_ACCESS_KEY + TABLESTORE_SECRET_KEY

go run ./cmd/sync_tables -push
```

查看命令行帮助：`go run ./cmd/sync_tables --help`（亦支持 `-h`、`-help`）

常用参数：

- `-push`：推送到远程（与 `-pull` 二选一，**必选其一**）
- `-table 名称`：指定目标。`-push` 模式可传“表名或索引名”；`-pull` 模式仅支持表名
- `-force`：表已存在则先删后建（**生产慎用**）
- `-config path`：`tables.yaml` 路径（默认与 `SIMPLEOTSGO_TABLES_PATH` 或 `./config/tables.yaml` 一致）
- `-pull`：从远程拉取并写回本地 YAML（与 `-push` 二选一）。**全量拉取**时只替换本次涉及的实例（`-instances` 或 YAML 里出现的实例）下的表，**其它实例的表定义保留**；带 `-table` 时只替换同名那一张表
- `-instances a,b`：拉取模式可选，显式指定实例列表（当本地 YAML 为空时很有用）
- `-dry-run`：演练模式——**不**执行 `DeleteTable`/`CreateTable`，**不**覆盖 `tables.yaml`；拉取时会把生成的 YAML 打印到标准输出（可配合重定向保存）

拉取示例：

```bash
go run ./cmd/sync_tables -pull
go run ./cmd/sync_tables -pull -table user
go run ./cmd/sync_tables -pull -instances tec05,tec06
go run ./cmd/sync_tables -push -table task_log_index_level
```

演练示例：

```bash
go run ./cmd/sync_tables -push -dry-run
go run ./cmd/sync_tables -pull -dry-run > /tmp/tables.preview.yaml
```

若设置 `TABLESTORE_ENDPOINT`，同步工具对该次任务**所有实例**使用该地址（多实例配置时请谨慎）。

## 进阶 API（可选）

```go
// 显式指定 endpoint（覆盖自动拼接）
op, err := simpleotsgo.NewWithEndpoint("ak", "sk", "https://xxx.cn-hangzhou.ots.aliyuncs.com")

// 显式指定 endpoint + tables.yaml 路径
op, err := simpleotsgo.NewWithConfig("ak", "sk", "https://xxx", "/data/config/tables.yaml")

// 调整重试参数
simpleotsgo.SetDefaultRetryConfig(simpleotsgo.RetryConfig{
	MaxRetries:     2,
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
	Multiplier:     2,
})
```

## License

MIT，详见 `LICENSE`。
