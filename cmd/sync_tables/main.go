// Package main 提供双向同步工具：
// 1) -push：将本地 tables.yaml 推送到远程 TableStore（建表）
// 2) -pull：将远程 TableStore 表结构拉取并写回本地 tables.yaml
// 3) -dry-run：不写远程、不写文件；推送仅 DescribeTable 演练，拉取将 YAML 输出到 stdout
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aliyun/aliyun-tablestore-go-sdk/v5/tablestore"
	"github.com/izut/simple-ots-go"
	"github.com/izut/simple-ots-go/config"
	"gopkg.in/yaml.v2"
)

// indexSyncTask 描述一个待同步的二级索引任务。
// 当用户通过 -table 传入索引名时，会解析为该结构并走 CreateIndex 流程。
type indexSyncTask struct {
	InstanceName string
	MainTable    config.TableConfig
	Index        config.Index
}

func main() {
	// tableFlag 指定目标表名或索引名；为空则处理全部表。
	tableFlag := flag.String("table", "", "指定目标表名或索引名（可选）")
	// forceFlag 仅在推送模式生效：表存在则先删后建。
	forceFlag := flag.Bool("force", false, "推送模式强制重建（表存在先删后建）")
	// pushFlag 为 true 时执行「本地 -> 远程」同步（须与 -pull 二选一）。
	pushFlag := flag.Bool("push", false, "推送模式：按本地 tables.yaml 在远程建表（须与 -pull 二选一）")
	// pullFlag 为 true 时启用“远程 -> 本地”拉取模式。
	pullFlag := flag.Bool("pull", false, "拉取模式：从远程 TableStore 回写到本地 tables.yaml（须与 -push 二选一）")
	// configPathFlag 指定 tables.yaml 路径；为空则走默认规则。
	configPathFlag := flag.String("config", "", "tables.yaml 路径（默认 SIMPLEOTSGO_TABLES_PATH 或 ./config/tables.yaml）")
	// instancesFlag 逗号分隔实例名。拉取模式下若本地无配置，可通过该参数指定实例。
	instancesFlag := flag.String("instances", "", "拉取模式可选：逗号分隔实例名，如 tec05,tec06")
	// dryRunFlag 为 true 时不改远程、不写文件：推送仅校验并 DescribeTable 演练（预定义列 / 缺失索引）；拉取将 YAML 打到标准输出。
	dryRunFlag := flag.Bool("dry-run", false, "演练模式：不写远程、不写文件；拉取时 YAML 输出到 stdout")

	// 注册帮助后设置 Usage，便于 flag.PrintDefaults 列出全部选项。
	flag.Usage = printSyncTablesHelp
	if helpRequested(os.Args[1:]) {
		printSyncTablesHelp()
		os.Exit(0)
	}
	flag.Parse()

	if *pushFlag && *pullFlag {
		log.Fatal("[sync_tables] 不能同时使用 -push 与 -pull，请任选其一（见 --help）")
	}
	if !*pushFlag && !*pullFlag {
		fmt.Fprintf(os.Stderr, "sync_tables: 必须指定 -push（推送到远程）或 -pull（拉取到本地），详见 --help\n\n")
		flag.Usage()
		os.Exit(2)
	}

	cfgPath := strings.TrimSpace(*configPathFlag)
	if cfgPath == "" {
		cfgPath = simpleotsgo.DefaultTablesConfigPath()
	}
	log.Printf("[sync_tables] 配置文件：%s", cfgPath)
	if *dryRunFlag {
		log.Println("[sync_tables] dry-run：推送不执行 DeleteTable/CreateTable/AddDefinedColumn/CreateIndex；拉取不写文件（YAML 输出到 stdout）")
	}

	accessKeyID, secret, err := loadAccessKeys()
	if err != nil {
		log.Fatalf("[sync_tables] %v", err)
	}

	if *pullFlag {
		if err := pullRemoteToLocal(cfgPath, strings.TrimSpace(*tableFlag), strings.TrimSpace(*instancesFlag), accessKeyID, secret, *dryRunFlag); err != nil {
			log.Fatalf("[sync_tables] 拉取失败：%v", err)
		}
		log.Println("[sync_tables] 拉取完成")
		return
	}

	// *pushFlag 为 true（已与 -pull 互斥校验）
	if err := pushLocalToRemote(cfgPath, strings.TrimSpace(*tableFlag), *forceFlag, accessKeyID, secret, *dryRunFlag); err != nil {
		log.Fatalf("[sync_tables] 推送失败：%v", err)
	}
	log.Println("[sync_tables] 推送完成")
}

// helpRequested 判断是否应打印帮助并退出（支持 -h / --help / -help）。
func helpRequested(args []string) bool {
	for _, a := range args {
		switch a {
		case "-h", "--help", "-help":
			return true
		}
	}
	return false
}

// printSyncTablesHelp 输出中文帮助与环境变量说明到标准输出。
func printSyncTablesHelp() {
	out := flag.CommandLine.Output()
	if out == nil {
		out = os.Stdout
	}
	fmt.Fprintf(out, `sync_tables — TableStore 与 tables.yaml 表结构同步工具

用法:
  go run ./cmd/sync_tables [选项]

`)
	fmt.Fprintf(out, "全部选项:\n")
	flag.PrintDefaults()
	fmt.Fprintf(out, `
模式与参数说明（重点）:
  必须且只能指定一种：-push 或 -pull（两者不能同时出现）。

  [-push 模式] 推送 local -> remote
    表已存在且未使用 -force 时：主键须与本地一致，否则报错；将用 AddDefinedColumn
    仅为远程补齐本地多出的预定义列（不删列、不改主键）。
    indexes 每项可写 indexType: global（默认，可省略）或 local（本地二级索引）。
    按表名推送时：远程缺索引会自动 CreateIndex；若 -table 为索引名则只同步该索引。
    生效参数:
      -push       开启推送（必需）
      -table      表名：同步表结构并补缺失索引；索引名：仅 CreateIndex 该索引
      -force      推送前先删后建（危险操作）
      -dry-run    仅演练，不执行 DeleteTable/CreateTable/AddDefinedColumn/CreateIndex
      -config     指定本地 tables.yaml 路径
    忽略参数:
      -instances  在推送模式下不会使用

  [-pull 模式] 拉取 remote -> local
    生效参数:
      -pull       开启拉取（必需）
      -table      仅拉取并替换同名表（拉取模式不支持按索引名）
      -instances  指定要拉取的实例列表（逗号分隔）
      -dry-run    不写文件，直接把生成 YAML 输出到 stdout
      -config     指定要写入（或预览目标）的本地 tables.yaml 路径
    拉取合并规则:
      - 带 -table：只替换同名表，其它表保留
      - 不带 -table：只替换本次拉取涉及实例下的表，其它实例保留

常用环境变量（与 SimpleOTSGo SDK 一致）:
  TABLESTORE_ACCESS_KEY_ID / TABLESTORE_ACCESS_KEY_SECRET
  或 TABLESTORE_ACCESS_KEY / TABLESTORE_SECRET_KEY
  TABLESTORE_AREA 或 SIMPLEOTSGO_TABLESTORE_AREA   区域，如 cn-hangzhou
  SIMPLEOTSGO_RUN_MODE 或 GO_ENV / APP_ENV / ENV   development=公网，production=VPC
  TABLESTORE_ENDPOINT   若设置则优先生效（多实例时请慎用）
  SIMPLEOTSGO_TABLES_PATH  tables.yaml 路径（与 -config 二选一）

示例:
  go run ./cmd/sync_tables --help
  go run ./cmd/sync_tables -push
  go run ./cmd/sync_tables -push -dry-run
  go run ./cmd/sync_tables -push -table task_log
  go run ./cmd/sync_tables -push -table task_log_index_level
  go run ./cmd/sync_tables -pull -table user
  go run ./cmd/sync_tables -pull -dry-run > preview.yaml
`)
}

// pushLocalToRemote 执行“本地 -> 远程”同步（建表）。
func pushLocalToRemote(cfgPath, targetTable string, force bool, accessKeyID, secret string, dryRun bool) error {
	log.Println("[sync_tables] 模式：推送（local -> remote）")
	if targetTable != "" {
		log.Printf("[sync_tables] 指定表：%s", targetTable)
	}
	if force {
		log.Println("[sync_tables] 强制模式：已存在的表将被删除后重建")
	}

	tablesCfg, err := config.LoadTablesConfig(cfgPath)
	if err != nil {
		return fmt.Errorf("读取或解析配置失败: %w", err)
	}
	log.Printf("[sync_tables] 共 %d 张表定义", len(tablesCfg.Tables))

	// 如果 -table 命中的是索引名，则切换到“索引同步模式”。
	if targetTable != "" {
		if idxTasks, isIndexTarget, err := resolveIndexSyncTasks(tablesCfg.Tables, targetTable); err != nil {
			return err
		} else if isIndexTarget {
			return pushIndexesByTarget(idxTasks, accessKeyID, secret, dryRun)
		}
	}

	byInstance := groupTablesByInstance(tablesCfg.Tables)
	instances := sortedInstanceKeys(byInstance)
	matchedTables := 0
	for _, instanceName := range instances {
		tables := byInstance[instanceName]
		log.Printf("[sync_tables] ---------- 实例 %s（%d 张表）----------", instanceName, len(tables))

		endpoint, err := simpleotsgo.SyncTableStoreEndpoint(instanceName)
		if err != nil {
			return fmt.Errorf("实例 %s 解析 endpoint 失败: %w", instanceName, err)
		}
		log.Printf("[sync_tables] endpoint: %s", endpoint)
		client := tablestore.NewClient(endpoint, instanceName, accessKeyID, secret)

		for i, tc := range tables {
			if targetTable != "" && tc.Name != targetTable {
				continue
			}
			matchedTables++
			log.Printf("[sync_tables] [%d/%d] %s", i+1, len(tables), tc.Name)
			var err error
			if dryRun {
				err = syncOneTableDryRun(client, tc, force)
			} else {
				err = syncOneTable(client, tc, force)
			}
			if err != nil {
				log.Printf("[sync_tables]   失败：%v", err)
			} else {
				log.Printf("[sync_tables]   成功")
			}
		}
	}
	if targetTable != "" && matchedTables == 0 {
		return fmt.Errorf("未找到表或索引: %s", targetTable)
	}
	return nil
}

// resolveIndexSyncTasks 判断 -table 是否命中索引名，并生成索引同步任务列表。
// 返回值：
// - tasks: 需要同步的索引任务；
// - isIndexTarget: true 表示 target 命中的是索引名；
// - err: 包含“同名索引在多表中重复”这类冲突错误。
func resolveIndexSyncTasks(tables []config.TableConfig, target string) ([]indexSyncTask, bool, error) {
	tasks := make([]indexSyncTask, 0)
	for _, t := range tables {
		for _, idx := range t.Indexes {
			if idx.Name != target {
				continue
			}
			tasks = append(tasks, indexSyncTask{
				InstanceName: t.InstanceName,
				MainTable:    t,
				Index:        idx,
			})
		}
	}
	if len(tasks) == 0 {
		return nil, false, nil
	}
	if len(tasks) > 1 {
		return nil, true, fmt.Errorf("索引名 %s 在多张表中重复，无法唯一定位，请改用表名或调整索引命名", target)
	}
	return tasks, true, nil
}

// pushIndexesByTarget 按索引任务执行 CreateIndex。
// 该流程用于“表已存在，仅补建二级索引”的场景。
func pushIndexesByTarget(tasks []indexSyncTask, accessKeyID, secret string, dryRun bool) error {
	log.Printf("[sync_tables] 检测到目标为索引名，切换索引同步模式，共 %d 个任务", len(tasks))
	for _, task := range tasks {
		endpoint, err := simpleotsgo.SyncTableStoreEndpoint(task.InstanceName)
		if err != nil {
			return fmt.Errorf("实例 %s 解析 endpoint 失败: %w", task.InstanceName, err)
		}
		log.Printf("[sync_tables] ---------- 实例 %s（索引同步）----------", task.InstanceName)
		log.Printf("[sync_tables] endpoint: %s", endpoint)
		client := tablestore.NewClient(endpoint, task.InstanceName, accessKeyID, secret)

		if dryRun {
			if err := syncOneIndexDryRun(client, task.MainTable, task.Index); err != nil {
				return err
			}
			continue
		}
		if err := syncOneIndex(client, task.MainTable, task.Index); err != nil {
			return err
		}
		log.Printf("[sync_tables] 索引同步成功：%s（主表 %s）", task.Index.Name, task.MainTable.Name)
	}
	return nil
}

// pullRemoteToLocal 执行“远程 -> 本地”同步（回写 YAML）。
func pullRemoteToLocal(cfgPath, targetTable, instancesArg, accessKeyID, secret string, dryRun bool) error {
	log.Println("[sync_tables] 模式：拉取（remote -> local）")
	if targetTable != "" {
		log.Printf("[sync_tables] 指定表：%s", targetTable)
	}

	// 先尝试读取现有 YAML，以便获取实例列表、并在 -table 模式下合并保留未改动表。
	existingCfg, _ := config.LoadTablesConfig(cfgPath)
	existingTables := make([]config.TableConfig, 0)
	if existingCfg != nil {
		existingTables = append(existingTables, existingCfg.Tables...)
	}

	instances := resolveInstances(existingTables, instancesArg)
	if len(instances) == 0 {
		return fmt.Errorf("未找到可拉取的实例，请在 tables.yaml 中配置 instanceName 或通过 -instances 指定")
	}

	remoteTables := make([]config.TableConfig, 0)
	for _, instanceName := range instances {
		endpoint, err := simpleotsgo.SyncTableStoreEndpoint(instanceName)
		if err != nil {
			return fmt.Errorf("实例 %s 解析 endpoint 失败: %w", instanceName, err)
		}
		log.Printf("[sync_tables] 实例 %s endpoint: %s", instanceName, endpoint)
		client := tablestore.NewClient(endpoint, instanceName, accessKeyID, secret)

		oneInstanceTables, err := pullOneInstanceTables(client, instanceName, targetTable)
		if err != nil {
			return fmt.Errorf("实例 %s 拉取失败: %w", instanceName, err)
		}
		remoteTables = append(remoteTables, oneInstanceTables...)
	}

	// 本次参与拉取的实例集合：全量拉取时只替换这些实例在 YAML 中的表，其它实例的配置保留。
	pulledInstanceSet := make(map[string]bool, len(instances))
	for _, in := range instances {
		pulledInstanceSet[in] = true
	}

	var finalTables []config.TableConfig
	if targetTable != "" {
		// 单表模式：仅替换同名表一条（其余表全部保留）。
		finalTables = mergeTablesByName(existingTables, remoteTables, targetTable)
	} else {
		// 全量模式：未拉取的实例保持本地原样；已拉取的实例以远程为准（可能增删表）。
		finalTables = mergePullPreserveOtherInstances(existingTables, remoteTables, pulledInstanceSet)
	}
	sortTables(finalTables)

	yamlBytes, err := yaml.Marshal(&config.TablesConfig{Tables: finalTables})
	if err != nil {
		return fmt.Errorf("序列化 YAML 失败: %w", err)
	}
	if dryRun {
		log.Printf("[sync_tables] [dry-run] 未写入文件；以下 YAML 共 %d 张表（目标路径 %s）\n", len(finalTables), cfgPath)
		if _, err := os.Stdout.Write(yamlBytes); err != nil {
			return fmt.Errorf("写出到 stdout 失败: %w", err)
		}
		return nil
	}
	if err := os.WriteFile(cfgPath, yamlBytes, 0o644); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}
	log.Printf("[sync_tables] 已写入 %d 张表到 %s", len(finalTables), cfgPath)
	return nil
}

// pullOneInstanceTables 拉取某个实例下的全部（或指定）主表结构。
func pullOneInstanceTables(client *tablestore.TableStoreClient, instanceName, targetTable string) ([]config.TableConfig, error) {
	tableNames := make([]string, 0)
	if targetTable != "" {
		tableNames = append(tableNames, targetTable)
	} else {
		resp, err := client.ListTable()
		if err != nil {
			return nil, fmt.Errorf("ListTable 失败: %w", err)
		}
		tableNames = append(tableNames, resp.TableNames...)
	}
	sort.Strings(tableNames)

	// 先收集所有描述信息，再过滤“索引表本体”（若 ListTable 返回了索引物理表名）。
	descByName := make(map[string]*tablestore.DescribeTableResponse, len(tableNames))
	indexTableNames := make(map[string]bool)
	for _, name := range tableNames {
		desc, err := client.DescribeTable(&tablestore.DescribeTableRequest{TableName: name})
		if err != nil {
			if targetTable != "" && isTableNotExistError(err) {
				// 指定单表但远程不存在时，不视为整体失败，返回空结果即可。
				return []config.TableConfig{}, nil
			}
			return nil, fmt.Errorf("DescribeTable(%s) 失败: %w", name, err)
		}
		descByName[name] = desc
		for _, idx := range desc.IndexMetas {
			indexTableNames[idx.IndexName] = true
		}
	}

	results := make([]config.TableConfig, 0, len(descByName))
	for _, name := range tableNames {
		if indexTableNames[name] {
			continue
		}
		desc := descByName[name]
		results = append(results, convertDescribeToYAMLTable(instanceName, desc))
	}
	return results, nil
}

// convertDescribeToYAMLTable 将 DescribeTable 响应转换为 YAML 表结构。
func convertDescribeToYAMLTable(instanceName string, desc *tablestore.DescribeTableResponse) config.TableConfig {
	table := config.TableConfig{
		Name:           desc.TableMeta.TableName,
		InstanceName:   instanceName,
		PrimaryKeys:    make([]config.PrimaryKey, 0, len(desc.TableMeta.SchemaEntry)),
		DefinedColumns: make([]config.DefinedColumn, 0, len(desc.TableMeta.DefinedColumns)),
		Indexes:        make([]config.Index, 0, len(desc.IndexMetas)),
	}

	// 构建列名->类型映射，供索引字段类型回填。
	typeByName := make(map[string]string)
	for _, pk := range desc.TableMeta.SchemaEntry {
		pkType := "STRING"
		if pk.Type != nil {
			pkType = primaryKeyTypeToString(*pk.Type)
		}
		table.PrimaryKeys = append(table.PrimaryKeys, config.PrimaryKey{
			Name: *pk.Name,
			Type: pkType,
		})
		typeByName[*pk.Name] = pkType
	}
	for _, col := range desc.TableMeta.DefinedColumns {
		colType := definedColumnTypeToString(col.ColumnType)
		table.DefinedColumns = append(table.DefinedColumns, config.DefinedColumn{
			Name: col.Name,
			Type: colType,
		})
		typeByName[col.Name] = colType
	}

	for _, idx := range desc.IndexMetas {
		indexCfg := config.Index{
			Name:            idx.IndexName,
			IndexType:       config.FormatIndexYAMLType(idx.IndexType == tablestore.IT_LOCAL_INDEX),
			PrimaryKeys:     make([]config.PrimaryKey, 0, len(idx.Primarykey)),
			DefinedColumns:  make([]config.DefinedColumn, 0, len(idx.DefinedColumns)),
			IncludeBaseData: false,
		}
		for _, pkName := range idx.Primarykey {
			indexCfg.PrimaryKeys = append(indexCfg.PrimaryKeys, config.PrimaryKey{
				Name: pkName,
				Type: fallbackType(typeByName[pkName]),
			})
		}
		for _, colName := range idx.DefinedColumns {
			indexCfg.DefinedColumns = append(indexCfg.DefinedColumns, config.DefinedColumn{
				Name: colName,
				Type: fallbackType(typeByName[colName]),
			})
		}
		table.Indexes = append(table.Indexes, indexCfg)
	}
	return table
}

// mergePullPreserveOtherInstances 在「全量 -pull」时合并结果。
// 规则：保留 instanceName 不在 pulledInstances 中的本地表；对曾拉取的实例，丢弃本地该实例下旧条目，改为使用 remote 中的列表。
func mergePullPreserveOtherInstances(existing, remote []config.TableConfig, pulledInstances map[string]bool) []config.TableConfig {
	merged := make([]config.TableConfig, 0, len(existing)+len(remote))
	for _, t := range existing {
		if pulledInstances[t.InstanceName] {
			continue
		}
		merged = append(merged, t)
	}
	merged = append(merged, remote...)
	return merged
}

// mergeTablesByName 在单表拉取时仅替换同名表，保留其他原有配置。
func mergeTablesByName(existing, pulled []config.TableConfig, targetName string) []config.TableConfig {
	merged := make([]config.TableConfig, 0, len(existing)+len(pulled))
	for _, t := range existing {
		if t.Name == targetName {
			continue
		}
		merged = append(merged, t)
	}
	merged = append(merged, pulled...)
	return merged
}

func groupTablesByInstance(tables []config.TableConfig) map[string][]config.TableConfig {
	byInstance := make(map[string][]config.TableConfig)
	for _, t := range tables {
		byInstance[t.InstanceName] = append(byInstance[t.InstanceName], t)
	}
	return byInstance
}

func sortedInstanceKeys(byInstance map[string][]config.TableConfig) []string {
	keys := make([]string, 0, len(byInstance))
	for k := range byInstance {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// resolveInstances 解析拉取目标实例列表。
// 优先级：-instances > 本地 tables.yaml 中出现的 instanceName。
func resolveInstances(existing []config.TableConfig, instancesArg string) []string {
	if instancesArg != "" {
		parts := strings.Split(instancesArg, ",")
		out := make([]string, 0, len(parts))
		seen := make(map[string]bool)
		for _, p := range parts {
			v := strings.TrimSpace(p)
			if v == "" || seen[v] {
				continue
			}
			seen[v] = true
			out = append(out, v)
		}
		sort.Strings(out)
		return out
	}

	seen := make(map[string]bool)
	out := make([]string, 0)
	for _, t := range existing {
		if t.InstanceName == "" || seen[t.InstanceName] {
			continue
		}
		seen[t.InstanceName] = true
		out = append(out, t.InstanceName)
	}
	sort.Strings(out)
	return out
}

func sortTables(tables []config.TableConfig) {
	sort.Slice(tables, func(i, j int) bool {
		if tables[i].InstanceName == tables[j].InstanceName {
			return tables[i].Name < tables[j].Name
		}
		return tables[i].InstanceName < tables[j].InstanceName
	})
}

func fallbackType(t string) string {
	if strings.TrimSpace(t) == "" {
		return "STRING"
	}
	return t
}

func primaryKeyTypeToString(t tablestore.PrimaryKeyType) string {
	switch t {
	case tablestore.PrimaryKeyType_INTEGER:
		return "INTEGER"
	case tablestore.PrimaryKeyType_BINARY:
		return "BINARY"
	default:
		return "STRING"
	}
}

func definedColumnTypeToString(t tablestore.DefinedColumnType) string {
	switch t {
	case tablestore.DefinedColumn_INTEGER:
		return "INTEGER"
	case tablestore.DefinedColumn_DOUBLE:
		return "DOUBLE"
	case tablestore.DefinedColumn_BOOLEAN:
		return "BOOLEAN"
	case tablestore.DefinedColumn_BINARY:
		return "BINARY"
	default:
		return "STRING"
	}
}

// loadAccessKeys 从环境变量读取 AK/SK。
// 优先使用 TABLESTORE_ACCESS_KEY_ID / TABLESTORE_ACCESS_KEY_SECRET，
// 否则回退到 TABLESTORE_ACCESS_KEY / TABLESTORE_SECRET_KEY。
func loadAccessKeys() (id, secret string, err error) {
	id = strings.TrimSpace(os.Getenv("TABLESTORE_ACCESS_KEY_ID"))
	secret = strings.TrimSpace(os.Getenv("TABLESTORE_ACCESS_KEY_SECRET"))
	if id != "" && secret != "" {
		return id, secret, nil
	}
	id = strings.TrimSpace(os.Getenv("TABLESTORE_ACCESS_KEY"))
	secret = strings.TrimSpace(os.Getenv("TABLESTORE_SECRET_KEY"))
	if id != "" && secret != "" {
		return id, secret, nil
	}
	return "", "", fmt.Errorf("请设置 TABLESTORE_ACCESS_KEY_ID + TABLESTORE_ACCESS_KEY_SECRET，或 TABLESTORE_ACCESS_KEY + TABLESTORE_SECRET_KEY")
}

// validateTableConfig 校验 YAML 中类型字段是否可被 SDK 接受。
func validateTableConfig(tc config.TableConfig) error {
	for _, pk := range tc.PrimaryKeys {
		if _, err := yamlPrimaryKeyType(pk.Type); err != nil {
			return fmt.Errorf("主键 %s: %w", pk.Name, err)
		}
	}
	for _, col := range tc.DefinedColumns {
		if _, err := yamlDefinedColumnType(col.Type); err != nil {
			return fmt.Errorf("属性列 %s: %w", col.Name, err)
		}
	}
	for _, idx := range tc.Indexes {
		if _, err := config.ParseIndexYAMLType(idx.IndexType); err != nil {
			return fmt.Errorf("索引 %s: %w", idx.Name, err)
		}
	}
	return nil
}

// syncOneTableDryRun 推送演练：不删不建不加列，仅校验并说明将执行的 DDL。
func syncOneTableDryRun(client *tablestore.TableStoreClient, tc config.TableConfig, force bool) error {
	if err := validateTableConfig(tc); err != nil {
		return err
	}
	if force {
		log.Printf("[sync_tables]   [dry-run] 将执行 DeleteTable（若表存在）: %s", tc.Name)
	} else {
		log.Printf("[sync_tables]   [dry-run] 未指定 -force，不会先删表")
	}
	desc, descErr := client.DescribeTable(&tablestore.DescribeTableRequest{TableName: tc.Name})
	if descErr != nil {
		if isTableNotExistError(descErr) {
			log.Printf("[sync_tables]   [dry-run] 远程不存在表 %s：将执行 CreateTable（主键%d 预定义列%d 索引%d）",
				tc.Name, len(tc.PrimaryKeys), len(tc.DefinedColumns), len(tc.Indexes))
			for _, idx := range tc.Indexes {
				log.Printf("[sync_tables]     [dry-run] 索引（%s）: %s", indexTypeLabel(idx.IndexType), idx.Name)
			}
			return nil
		}
		return fmt.Errorf("DescribeTable 失败: %w", descErr)
	}
	// 远程表已存在
	if force {
		log.Printf("[sync_tables]   [dry-run] 远程已存在表 %s：配合 -force 将先删后建", tc.Name)
		log.Printf("[sync_tables]   [dry-run] 将 CreateTable: 主键%d 预定义列%d 索引%d", len(tc.PrimaryKeys), len(tc.DefinedColumns), len(tc.Indexes))
		for _, idx := range tc.Indexes {
			log.Printf("[sync_tables]     [dry-run] 索引（%s）: %s", indexTypeLabel(idx.IndexType), idx.Name)
		}
		return nil
	}
	if err := assertPrimaryKeysMatchRemote(desc, tc); err != nil {
		return err
	}
	toAdd, err := diffDefinedColumnsToAdd(desc.TableMeta.DefinedColumns, tc.DefinedColumns)
	if err != nil {
		return err
	}
	if len(toAdd) == 0 {
		log.Printf("[sync_tables]   [dry-run] 远程已存在表 %s：主键一致，无新增预定义列", tc.Name)
	} else {
		log.Printf("[sync_tables]   [dry-run] 远程已存在表 %s：将 AddDefinedColumn 补齐 %d 列", tc.Name, len(toAdd))
		for _, c := range toAdd {
			log.Printf("[sync_tables]     [dry-run]   + %s (%s)", c.Name, c.Type)
		}
	}
	dryRunMissingIndexes(tc, desc)
	return nil
}

// syncOneTable 创建单表（可选强制删表重建），或表已存在时补齐预定义列与缺失的二级索引。
func syncOneTable(client *tablestore.TableStoreClient, tc config.TableConfig, force bool) error {
	if err := validateTableConfig(tc); err != nil {
		return err
	}
	if force {
		log.Printf("[sync_tables]   强制：尝试删除已存在表 %s ...", tc.Name)
		_, delErr := client.DeleteTable(&tablestore.DeleteTableRequest{TableName: tc.Name})
		if delErr != nil {
			if !isTableNotExistError(delErr) {
				return fmt.Errorf("删除表失败: %w", delErr)
			}
			log.Printf("[sync_tables]   表不存在，跳过删除")
		} else {
			log.Printf("[sync_tables]   已删除，等待元数据生效...")
			time.Sleep(2 * time.Second)
		}
		return createTableFromYAML(client, tc)
	}

	desc, descErr := client.DescribeTable(&tablestore.DescribeTableRequest{TableName: tc.Name})
	if isTableNotExistError(descErr) {
		return createTableFromYAML(client, tc)
	}
	if descErr != nil {
		return fmt.Errorf("DescribeTable 失败: %w", descErr)
	}

	if err := assertPrimaryKeysMatchRemote(desc, tc); err != nil {
		return err
	}
	toAdd, err := diffDefinedColumnsToAdd(desc.TableMeta.DefinedColumns, tc.DefinedColumns)
	if err != nil {
		return err
	}
	if len(toAdd) > 0 {
		log.Printf("[sync_tables]     远程表已存在，AddDefinedColumn 补齐 %d 列 ...", len(toAdd))
		for _, c := range toAdd {
			log.Printf("[sync_tables]       + %s (%s)", c.Name, c.Type)
		}
		if err := applyAddDefinedColumns(client, tc.Name, toAdd); err != nil {
			return fmt.Errorf("AddDefinedColumn 失败: %w", err)
		}
	} else {
		log.Printf("[sync_tables]     远程表已存在，预定义列已对齐")
	}
	return ensureLocalIndexesOnRemote(client, tc, desc)
}

// createTableFromYAML 按本地配置执行 CreateTable（含 YAML 中的二级索引元数据）。
func createTableFromYAML(client *tablestore.TableStoreClient, tc config.TableConfig) error {
	tableMeta := &tablestore.TableMeta{TableName: tc.Name}
	for _, pk := range tc.PrimaryKeys {
		pkType, err := yamlPrimaryKeyType(pk.Type)
		if err != nil {
			return fmt.Errorf("主键 %s: %w", pk.Name, err)
		}
		tableMeta.AddPrimaryKeyColumn(pk.Name, pkType)
		log.Printf("[sync_tables]     主键: %s (%s)", pk.Name, pk.Type)
	}
	for _, col := range tc.DefinedColumns {
		dt, err := yamlDefinedColumnType(col.Type)
		if err != nil {
			return fmt.Errorf("属性列 %s: %w", col.Name, err)
		}
		tableMeta.AddDefinedColumn(col.Name, dt)
		log.Printf("[sync_tables]     属性列: %s (%s)", col.Name, col.Type)
	}

	req := &tablestore.CreateTableRequest{
		TableMeta: tableMeta,
		TableOption: &tablestore.TableOption{
			TimeToAlive: -1,
			MaxVersion:  1,
		},
		ReservedThroughput: &tablestore.ReservedThroughput{Readcap: 0, Writecap: 0},
	}

	for _, idx := range tc.Indexes {
		im := &tablestore.IndexMeta{IndexName: idx.Name}
		for _, pk := range idx.PrimaryKeys {
			im.AddPrimaryKeyColumn(pk.Name)
		}
		for _, col := range idx.DefinedColumns {
			im.AddDefinedColumn(col.Name)
		}
		if err := applyIndexMetaFromYAML(im, idx.IndexType); err != nil {
			return fmt.Errorf("索引 %s: %w", idx.Name, err)
		}
		req.AddIndexMeta(im)
		log.Printf("[sync_tables]     索引: %s（%s）", idx.Name, indexTypeLabel(idx.IndexType))
	}

	_, err := client.CreateTable(req)
	return err
}

// syncOneIndexDryRun 索引同步演练：不实际调用 CreateIndex，仅校验与远程状态检查。
func syncOneIndexDryRun(client *tablestore.TableStoreClient, tableCfg config.TableConfig, indexCfg config.Index) error {
	log.Printf("[sync_tables]   [dry-run] 索引目标: %s（主表 %s）", indexCfg.Name, tableCfg.Name)
	// 先确认主表是否存在，并检查索引是否已存在。
	desc, err := client.DescribeTable(&tablestore.DescribeTableRequest{TableName: tableCfg.Name})
	if err != nil {
		return fmt.Errorf("DescribeTable(%s) 失败: %w", tableCfg.Name, err)
	}
	for _, idx := range desc.IndexMetas {
		if idx.IndexName == indexCfg.Name {
			log.Printf("[sync_tables]   [dry-run] 远程已存在同名索引 %s，实际执行时 CreateIndex 可能报重复", indexCfg.Name)
			return nil
		}
	}
	log.Printf("[sync_tables]   [dry-run] 远程不存在索引 %s，将执行 CreateIndex", indexCfg.Name)
	return nil
}

// syncOneIndex 为已存在主表创建二级索引。
func syncOneIndex(client *tablestore.TableStoreClient, tableCfg config.TableConfig, indexCfg config.Index) error {
	indexMeta := &tablestore.IndexMeta{IndexName: indexCfg.Name}
	for _, pk := range indexCfg.PrimaryKeys {
		indexMeta.AddPrimaryKeyColumn(pk.Name)
	}
	for _, col := range indexCfg.DefinedColumns {
		indexMeta.AddDefinedColumn(col.Name)
	}
	if err := applyIndexMetaFromYAML(indexMeta, indexCfg.IndexType); err != nil {
		return fmt.Errorf("索引 %s: %w", indexCfg.Name, err)
	}

	req := &tablestore.CreateIndexRequest{
		MainTableName:   tableCfg.Name,
		IndexMeta:       indexMeta,
		IncludeBaseData: indexCfg.IncludeBaseData,
	}
	_, err := client.CreateIndex(req)
	return err
}

// yamlPrimaryKeyType 将 YAML 类型字符串转换为主键类型。
func yamlPrimaryKeyType(s string) (tablestore.PrimaryKeyType, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "STRING":
		return tablestore.PrimaryKeyType_STRING, nil
	case "INTEGER", "INT":
		return tablestore.PrimaryKeyType_INTEGER, nil
	case "BINARY":
		return tablestore.PrimaryKeyType_BINARY, nil
	default:
		return 0, fmt.Errorf("不支持的主键类型: %q", s)
	}
}

// yamlDefinedColumnType 将 YAML 类型字符串转换为预定义列类型。
func yamlDefinedColumnType(s string) (tablestore.DefinedColumnType, error) {
	switch strings.ToUpper(strings.TrimSpace(s)) {
	case "STRING":
		return tablestore.DefinedColumn_STRING, nil
	case "INTEGER", "INT":
		return tablestore.DefinedColumn_INTEGER, nil
	case "BOOLEAN", "BOOL":
		return tablestore.DefinedColumn_BOOLEAN, nil
	case "DOUBLE", "FLOAT":
		return tablestore.DefinedColumn_DOUBLE, nil
	case "BINARY", "BLOB":
		return tablestore.DefinedColumn_BINARY, nil
	default:
		return 0, fmt.Errorf("不支持的属性列类型: %q", s)
	}
}

// isTableNotExistError 判断是否为“表不存在”错误。
func isTableNotExistError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "notexist") ||
		strings.Contains(msg, "otsobjectnotexist") ||
		strings.Contains(msg, "object not exist")
}
