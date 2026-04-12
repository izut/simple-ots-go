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
	// 地域在 tables.yaml 各表的 regionId 中配置（本示例假设已配置 cn-hangzhou 等）。
	// 开发模式走公网，生产模式走 VPC。
	_ = os.Setenv("APP_ENV", "development")
	// 可选：指定 tables.yaml，默认 ./config/tables.yaml。
	_ = os.Setenv("SIMPLEOTSGO_TABLES_PATH", "./config/tables.yaml")
	// 需提供 AK/SK（两组选其一）。
	_ = os.Setenv("TABLESTORE_ACCESS_KEY_ID", "your-access-key")
	_ = os.Setenv("TABLESTORE_ACCESS_KEY_SECRET", "your-secret-key")

	// AK/SK 从环境变量读取；instanceName 与 regionId 均从 tables.yaml 读取（未写 regionId 且未设 TABLESTORE_ENDPOINT 将无法连接）。
	userTable, err := simpleotsgo.Table("user")
	if err != nil {
		panic(err)
	}

	// Create
	_, err = userTable.PutRow(map[string]interface{}{
		"uid":       "user_00001",
		"user_name": "张三",
		"email":     "zhangsan@example.com",
		"status":    int64(1),
	}, nil)
	if err != nil {
		panic(err)
	}

	// Read
	row, err := userTable.GetRow(map[string]interface{}{"uid": "user_00001"}, nil, 0)
	if err != nil {
		panic(err)
	}
	fmt.Printf("row=%+v\n", row)

	// Update：RowData 含主键与待 Put 列；可选 DeleteColumns、IncrementColumns；第二个参数为 RowCondition，nil 表示默认「行须已存在」。
	err = userTable.UpdateRow(simpleotsgo.UpdateData{
		RowData: map[string]interface{}{
			"uid":       "user_00001",
			"user_name": "张三-已更新",
		},
	}, nil)
	if err != nil {
		panic(err)
	}

	// Delete：返回 DeleteRowResponse（CU、RequestId 等），不需要时可 `_` 丢弃。
	_, err = userTable.DeleteRow(map[string]interface{}{"uid": "user_00001"})
	if err != nil {
		panic(err)
	}
}
```

## 关键规则（必看）

- `instanceName` 来自 `tables.yaml` 中对应表配置
- 每张表须在 `tables.yaml` 中配置 `regionId`（地域 ID，如 `cn-hangzhou`），SDK 不再从环境变量读取地域
- `APP_ENV=development` 使用公网 endpoint
- `APP_ENV=production` 使用 VPC endpoint
- `GO_ENV=production` 与 `SIMPLEOTSGO_RUN_MODE=production` 与 `APP_ENV=production` 等价参与判定（与常见 Go 项目习惯一致）
- 显式设置 `TABLESTORE_ENDPOINT` 时，优先使用显式值

## 同步表结构（CLI）

支持双向同步，**必须显式指定一种方向**：

- `-push`：`tables.yaml -> TableStore`（创建/重建表）
- `-pull`：`TableStore -> tables.yaml`（回写远程表结构）

```bash
export APP_ENV=development   # 生产改 production 或 export GO_ENV=production 
export TABLESTORE_ACCESS_KEY_ID=...
export TABLESTORE_ACCESS_KEY_SECRET=...
# 或使用：TABLESTORE_ACCESS_KEY + TABLESTORE_SECRET_KEY

go run ./cmd/sync_tables -push
```

查看命令行帮助：`go run ./cmd/sync_tables --help`（亦支持 `-h`、`-help`）

### 在任意项目直接使用已发布版本

如果你不在本仓库目录，也可以直接使用发布 tag 的 `sync_tables`。

方式一：安装后长期使用（推荐）

```bash
go install github.com/izut/simple-ots-go/cmd/sync_tables@v1.0.alpha
sync_tables -push -config ./config/tables.yaml
```

方式二：不安装，按版本单次运行

```bash
go run github.com/izut/simple-ots-go/cmd/sync_tables@v1.0.alpha -push -config ./config/tables.yaml
```

说明：

- 以上命令中的 `v1.0.alpha` 可替换为你要使用的发布 tag
- `-config` 路径相对于当前执行目录；不传时默认 `./config/tables.yaml`
- 环境变量要求与本仓库内运行一致（如 `TABLESTORE_ACCESS_KEY_ID/SECRET` 等）；运行 SDK 时地域写在 `tables.yaml` 的 `regionId`；`sync_tables -pull` 在空 YAML 场景可用 `-regionId` 或 `TABLESTORE_ENDPOINT`

常用参数：

- `-push`：推送到远程（与 `-pull` 二选一，**必选其一**）
- `-table 名称`：指定目标。`-push` 模式可传“表名或索引名”；`-pull` 模式仅支持表名
- `-force`：表已存在则先删后建（**生产慎用**）
- `-config path`：`tables.yaml` 路径（默认与 `SIMPLEOTSGO_TABLES_PATH` 或 `./config/tables.yaml` 一致）
- `-pull`：从远程拉取并写回本地 YAML（与 `-push` 二选一）。**全量拉取**时只替换本次涉及的实例（`-instances` 或 YAML 里出现的实例）下的表，**其它实例的表定义保留**；带 `-table` 时只替换同名那一张表
- `-instances a,b`：拉取模式可选，显式指定实例列表（当本地 YAML 为空时很有用；此时请同时传 `-regionId cn-hangzhou` 等，或设置 `TABLESTORE_ENDPOINT`）
- `-regionId`：拉取专用；当本地 YAML 尚未包含某实例的 `regionId` 时用于连接远程并写回该字段（与 `-instances` 搭配引导空文件时常用）
- `-dry-run`：演练模式——**不**执行 `DeleteTable`/`CreateTable`，**不**覆盖 `tables.yaml`；拉取时会把生成的 YAML 打印到标准输出（可配合重定向保存）

拉取示例：

```bash
go run ./cmd/sync_tables -pull
go run ./cmd/sync_tables -pull -table user
go run ./cmd/sync_tables -pull -instances tec05,tec06 -regionId cn-hangzhou
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