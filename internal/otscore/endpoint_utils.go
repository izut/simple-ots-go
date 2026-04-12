package otscore

import (
	"fmt"
	"os"
	"strings"
)

const (
	// EndpointModeDevelopment 开发模式，使用公网 endpoint。
	EndpointModeDevelopment = "development"
	// EndpointModeProduction 生产模式，使用 VPC endpoint。
	EndpointModeProduction = "production"
)

// NormalizeRunMode 归一化运行模式。
func NormalizeRunMode(candidates ...string) string {
	for _, c := range candidates {
		switch strings.ToLower(strings.TrimSpace(c)) {
		case "production", "prod":
			return EndpointModeProduction
		case "development", "dev":
			return EndpointModeDevelopment
		}
	}
	return EndpointModeDevelopment
}

// BuildEndpoint 按实例名、地域 ID（RegionId，如 cn-hangzhou）和运行模式拼接 endpoint。
func BuildEndpoint(instanceName, regionID, mode string) (string, error) {
	instanceName = strings.TrimSpace(instanceName)
	regionID = strings.TrimSpace(regionID)
	if instanceName == "" {
		return "", fmt.Errorf("instance name is empty")
	}
	if regionID == "" {
		return "", fmt.Errorf("regionId is required when endpoint is not explicitly set")
	}
	if mode == EndpointModeProduction {
		return fmt.Sprintf("https://%s.%s.vpc.tablestore.aliyuncs.com", instanceName, regionID), nil
	}
	return fmt.Sprintf("https://%s.%s.tablestore.aliyuncs.com", instanceName, regionID), nil
}

// ResolveSyncEndpointWithRegionId 解析同步工具连接 endpoint。
// 若未设置 TABLESTORE_ENDPOINT，则必须使用非空的 regionID（由调用方从 tables.yaml 汇总传入）；不再从环境变量读取地域。
func ResolveSyncEndpointWithRegionId(instanceName, regionID string) (string, error) {
	if e := strings.TrimSpace(os.Getenv("TABLESTORE_ENDPOINT")); e != "" {
		return e, nil
	}
	regionID = strings.TrimSpace(regionID)
	if regionID == "" {
		return "", fmt.Errorf("regionId is required when TABLESTORE_ENDPOINT is not set (configure regionId in tables.yaml)")
	}
	mode := NormalizeRunMode(
		os.Getenv("SIMPLEOTSGO_RUN_MODE"),
		os.Getenv("APP_ENV"),
		os.Getenv("ENV"),
		os.Getenv("GO_ENV"),
	)
	return BuildEndpoint(instanceName, regionID, mode)
}
