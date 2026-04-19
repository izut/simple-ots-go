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

	// Read：第二个参数为列裁剪，nil 表示不限制列（MaxVersion 在操作器内固定为 1）。
	row, err := userTable.GetRow(map[string]interface{}{"uid": "user_00001"}, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("row=%+v\n", row)

	// Update：RowData 含主键与待 Put 列；可选 DeleteColumns、IncrementColumns；第二个参数为 RowCondition，nil 表示默认「行须已存在」。
	_, err = userTable.UpdateRow(simpleotsgo.UpdateData{
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
go install github.com/izut/simple-ots-go/cmd/sync_tables@v1.0.3
sync_tables -push -config ./config/tables.yaml

# 单次运行
go run github.com/izut/simple-ots-go/cmd/sync_tables@v1.0.3 -pull -table "*" -instances tec05 -regionId cn-hangzhou
```

- `v1.0.3` 可替换为你要使用的 tag
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

## `otscore.SimpleTableOperator` 与行级 API

实现类型定义在 `internal/otscore`，对外通过 `simpleotsgo` 包以**类型别名**暴露（二者为同一类型，可混用文档）：

```go
// simpleotsgo.go 中：type SimpleTableOperator = otscore.SimpleTableOperator
```

- **业务项目（推荐）**：只依赖模块根路径，使用 `simpleotsgo.Table("表名")` 或 `(*Operator).Table("表名")` 得到 `*simpleotsgo.SimpleTableOperator`。
- **同仓库 / 需直接引用内部包**：`import "github.com/izut/simple-ots-go/internal/otscore"`，使用 `otscore.Table(表名, endpoint, accessKeyID, accessKeySecret)`；表结构须已由 `otscore.InitSimpleOperator` / `RegisterTable` 或配置加载流程注册。

主键列名、顺序以 `tables.yaml` 为准；行数据中的 `*_json` 字段会按 SDK 约定编码/解码（与 `PutRow` / `GetRow` 一致）。

### 获取操作器

| 方式 | 说明 |
|------|------|
| `simpleotsgo.Table(tableName)` | 使用环境变量中的 AK/SK 与默认 `tables.yaml`，返回绑定该表的操作器。 |
| `(*simpleotsgo.Operator).Table(tableName)` | 在已 `New` / `NewWithConfig` 得到的 `Operator` 上按表名取操作器。 |
| `otscore.Table(tableName, endpoint, ak, sk)` | 内部包：显式传入 endpoint 与密钥；`endpoint` 常来自 `TABLESTORE_ENDPOINT` 或自建拼接。 |

### 方法一览（`SimpleTableOperator`）

| 方法 | 作用摘要 |
|------|----------|
| `GetRow(pk, columnsToGet)` | 按主键读一行，返回 `map[string]interface{}`（已解析 `*_json`）；`columnsToGet` 为 `nil`/空表示不裁剪列。 |
| `PutRow(data, cond)` | 写入一行；`cond` 为 `nil` 等价 `IGNORE`；返回 `*tablestore.PutRowResponse`（含 CU、主键等）。 |
| `UpdateRow(data, cond)` | `UpdateData` 含 `RowData`（主键 + 待 Put 列）、可选 `DeleteColumns` / `IncrementColumns`；`cond==nil` 为「行须已存在」。 |
| `DeleteRow(pk)` | 删除一行，默认存在性期望 `IGNORE`；返回 `*tablestore.DeleteRowResponse`。 |
| `BatchGetRows(rows, columnsToGet)` | 同表批量 `GetRow`，返回 `[]BatchGetRowItem`（每行成功/失败与 CU）。 |
| `BatchPutRows(rows, cond, returnType)` | 批量 Put，全表共用 `cond` 与 `returnType`；返回本表 `[]tablestore.RowResult`（与提交行顺序一致）。 |
| `BatchWriteRows(actions)` | `BatchWriteAction` 内 `PutRows` / `UpdateRows` / `DeleteRows` 及可选 `PutCond`/`UpdateCond`/`DeleteCond`；返回本表 `[]RowResult`（顺序与 Put→Update→Delete 拼接一致）。 |
| `BatchWriteChanges(changes)` | 直接提交已构造好的 `[]tablestore.RowChange`（每条的 `TableName` 须为当前表）。 |
| `GetRange(startPK, endPK, direction, limit)` | 范围读；边界为「主键列名 → 值」的 map，支持 `simpleotsgo.INF_MIN` / `INF_MAX`。返回 **四元组**：`(CU, nextStartPKMap, rows, err)` — `rows` 为 `[]map[string]interface{}`（已 `RowToMap`）；`nextStartPKMap` 为下一页起点（无下一页时可能为空 map）；`CU` 可能为 `nil`。 |
| `GetRangeWithPrimaryKeys(startPK, endPK, opts)` | 已构造 `*tablestore.PrimaryKey` 边界与 `*GetRangeOptions` 时使用；返回 `(CU, nextStartPK *tablestore.PrimaryKey, rows []map, err)`。 |
| `GetTableName` / `GetInstanceName` / `GetRegionId` | 元信息读取。 |

### 使用示例片段

```go
import (
	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	sog "github.com/izut/simple-ots-go"
)

op, err := sog.Table("task_log")
if err != nil {
	panic(err)
}

// 范围查询：续扫时将上一次返回的 nextStartPKMap 作为新的 startPK（与文档「前闭后开」语义一致）。
cu, nextPK, rows, err := op.GetRange(
	map[string]interface{}{"task_id": "t1", "timestamp": sog.INF_MIN},
	map[string]interface{}{"task_id": "t1", "timestamp": sog.INF_MAX},
	sog.FORWARD,
	100,
)
if err != nil {
	panic(err)
}
_ = cu
_ = nextPK
_ = rows

// 批量 Put：与 PutRow 相同的 map 语义；IGNORE 与 ReturnType 与官方 SDK 一致。
rr, err := op.BatchPutRows([]map[string]interface{}{
	{"task_id": "x", "timestamp": int64(1), "status": "ok"},
}, sog.IGNORE, tablestore.ReturnType_RT_NONE)
if err != nil {
	panic(err)
}
for i, r := range rr {
	_ = i
	_ = r.IsSucceed
}

// 混合批量写
_, err = op.BatchWriteRows(sog.BatchWriteAction{
	PutRows:    []map[string]interface{}{ /* ... */ },
	UpdateRows: nil,
	DeleteRows: nil,
	PutCond:    sog.IGNORE,
})
```

更多可运行示例见仓库 `examples/basic_usage.go`。

## License

MIT，详见 `LICENSE`。