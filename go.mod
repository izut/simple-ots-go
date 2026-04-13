module github.com/izut/simple-ots-go

go 1.22

// 撤回单个有 Bug 的版本
retract v1.0.0-a
require (
	github.com/aliyun/aliyun-tablestore-go-sdk/v5 v5.0.6
	gopkg.in/yaml.v2 v2.2.2
)

require github.com/golang/protobuf v1.3.2 // indirect
