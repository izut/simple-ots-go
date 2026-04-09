package simpleotsgo

import (
	"time"
)

// RetryConfig 定义重试策略配置。
// 该配置用于 SDK 内部所有需要访问远端 TableStore 的请求。
type RetryConfig struct {
	// MaxRetries 最大重试次数（不含首次请求）。
	// 例如 MaxRetries=2 表示“首次请求 + 最多 2 次重试”。
	MaxRetries int
	// InitialBackoff 首次重试前的等待时间。
	InitialBackoff time.Duration
	// MaxBackoff 最大退避时间上限，防止等待时间无限增长。
	MaxBackoff time.Duration
	// Multiplier 退避倍数，每次重试后等待时间按该倍数增长。
	Multiplier float64
}

var defaultRetryConfig = RetryConfig{
	MaxRetries:     2,
	InitialBackoff: 100 * time.Millisecond,
	MaxBackoff:     2 * time.Second,
	Multiplier:     2,
}

// SetDefaultRetryConfig 设置全局默认重试配置。
// 当参数不合法时，函数会自动回退到安全默认值，避免运行时出现异常行为。
func SetDefaultRetryConfig(cfg RetryConfig) {
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = defaultRetryConfig.InitialBackoff
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = defaultRetryConfig.MaxBackoff
	}
	if cfg.Multiplier < 1 {
		cfg.Multiplier = defaultRetryConfig.Multiplier
	}
	defaultRetryConfig = cfg
}

// withRetry 在失败时按退避策略执行重试。
// 这里不做复杂错误分类，而是对网络抖动等瞬态失败提供兜底能力。
func withRetry(action func() error) error {
	backoff := defaultRetryConfig.InitialBackoff
	var err error
	for i := 0; i <= defaultRetryConfig.MaxRetries; i++ {
		err = action()
		if err == nil {
			return nil
		}
		// 最后一次失败后直接返回，不再等待。
		if i == defaultRetryConfig.MaxRetries {
			break
		}
		time.Sleep(backoff)
		next := time.Duration(float64(backoff) * defaultRetryConfig.Multiplier)
		if next > defaultRetryConfig.MaxBackoff {
			next = defaultRetryConfig.MaxBackoff
		}
		backoff = next
	}
	return err
}
