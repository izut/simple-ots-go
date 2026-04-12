package otscore

import (
	"sync"
	"time"
)

type RetryConfig struct {
	MaxRetries     int
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Multiplier     float64
}

var (
	retryConfigMu      sync.RWMutex
	defaultRetryConfig = RetryConfig{
		MaxRetries:     2,
		InitialBackoff: 100 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
		Multiplier:     2,
	}
)

func SetDefaultRetryConfig(cfg RetryConfig) {
	retryConfigMu.Lock()
	defer retryConfigMu.Unlock()

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

func getRetryConfig() RetryConfig {
	retryConfigMu.RLock()
	defer retryConfigMu.RUnlock()
	return defaultRetryConfig
}

func withRetry(action func() error) error {
	cfg := getRetryConfig()
	backoff := cfg.InitialBackoff
	var err error
	for i := 0; i <= cfg.MaxRetries; i++ {
		err = action()
		if err == nil {
			return nil
		}
		if i == cfg.MaxRetries {
			break
		}
		time.Sleep(backoff)
		next := time.Duration(float64(backoff) * cfg.Multiplier)
		if next > cfg.MaxBackoff {
			next = cfg.MaxBackoff
		}
		backoff = next
	}
	return err
}
