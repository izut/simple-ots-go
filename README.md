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
- 主表可选 `dataLifeCycle`（单位：秒），默认 `-1`（永不过期）；该参数仅作用于主表 `CreateTable.TableOption.TimeToAlive`
- `APP_ENV=development` 使用公网 endpoint
- `APP_ENV=production` 使用 VPC endpoint
- `GO_ENV=production` 与 `SIMPLEOTSGO_RUN_MODE=production` 与 `APP_ENV=production` 等价参与判定（与常见 Go 项目习惯一致）
- 显式设置 `TABLESTORE_ENDPOINT` 时，优先使用显式值

## 同步表结构（CLI）

支持双向同步（**必须二选一**）：

- `-push`：`tables.yaml -> TableStore`（建表/补列/补索引）
- `-pull`：`TableStore -> tables.yaml`（回写远程结构）

### 环境准备

```bash
export APP_ENV=development     # 生产环境改 production（或 GO_ENV=production）
export TABLESTORE_ACCESS_KEY_ID=...
export TABLESTORE_ACCESS_KEY_SECRET=...
# 或使用：TABLESTORE_ACCESS_KEY + TABLESTORE_SECRET_KEY
```

### 快速用法

```bash
# 推送全部表
go run ./cmd/sync_tables -push

# 拉取全部表
go run ./cmd/sync_tables -pull
```

查看完整参数：`go run ./cmd/sync_tables --help`（同样支持 `-h` / `-help`）

### 参数说明（新版）

- `-push` / `-pull`：方向互斥，且必须指定一个
- `-table <name|*>`：
  - `-table "*"`：表示全部表（在 `-push` / `-pull` 都生效）
  - `-push` 时可传索引名：仅执行该索引 `CreateIndex`
  - `-pull` 时仅支持主表名（不支持按索引名）
- `-instance <name>`：单实例别名，等价 `-instances <name>`
- `-instances a,b`：按实例过滤；在 `-push` / `-pull` 都生效
- `-regionId`：拉取时地域回退（本地 YAML 缺该实例 `regionId` 时使用）
- `-regionid`：`-regionId` 的全小写别名
- `-force`：`-push` 模式下先删后建（高风险）
- `-dry-run`：演练模式（不改远程、不写本地文件）
- `-config <path>`：`tables.yaml` 路径（默认 `SIMPLEOTSGO_TABLES_PATH` 或 `./config/tables.yaml`）

### 行为规则（重点）

- `-table "*"` 与不传 `-table` 等价，都是“全部表”
- 指定 `-instance/-instances` 时，只处理这些实例下的表（push/pull 都一样）
- `-pull` 合并策略：
  - 指定 `-table`：只替换同名表，其他表保留
  - 全量（未指定具体表）：只替换本次涉及实例下的表，其他实例保留
- 若设置 `TABLESTORE_ENDPOINT`，本次任务的所有实例都会共用该 endpoint（多实例请谨慎）

### 与数据生命周期配合

主表支持 `dataLifeCycle`（秒）字段，默认 `-1`（永不过期）：

```yaml
tables:
  - name: task_log
    instanceName: bigots
    regionId: cn-hangzhou
    dataLifeCycle: -1
```

- `-push` 时用于 `CreateTable.TableOption.TimeToAlive`
- `-pull` 时会把远端 `TimeToAlive` 回填到 `tables.yaml`

### 常用示例

```bash
# 推送某实例全部表
go run ./cmd/sync_tables -push -table "*" -instances tec05

# 推送单表
go run ./cmd/sync_tables -push -table task_log

# 推送单索引（仅 push 支持）
go run ./cmd/sync_tables -push -table task_log_index_level

# 拉取某实例全部表（本地空 YAML 场景）
go run ./cmd/sync_tables -pull -table "*" -instance tec05 -regionid cn-hangzhou

# 拉取单表
go run ./cmd/sync_tables -pull -table user

# 演练模式
go run ./cmd/sync_tables -push -dry-run
go run ./cmd/sync_tables -pull -dry-run > /tmp/tables.preview.yaml
```

### 在任意项目使用发布版本

```bash
# 安装后长期使用
go install github.com/izut/simple-ots-go/cmd/sync_tables@v1.0.alpha
sync_tables -push -config ./config/tables.yaml

# 单次运行
go run github.com/izut/simple-ots-go/cmd/sync_tables@v1.0.alpha -pull -table "*" -instances tec05 -regionId cn-hangzhou
```

- `v1.0.alpha` 可替换为你要使用的 tag
- `-config` 为执行目录相对路径（不传默认 `./config/tables.yaml`）

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