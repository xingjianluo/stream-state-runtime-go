package streamruntime

import (
	"fmt"
	"time"
)

// 默认参数，零值配置字段会回退到这些值。
const (
	defaultWindowSize         = time.Minute
	defaultAllowedLateness    = 10 * time.Second
	defaultMaxPendingOutputs  = 16
	defaultIngestBuffer       = 64
	defaultCheckpointInterval = time.Second
	defaultWatermarkTick      = 20 * time.Millisecond
)

// NewConfig 构造经过校验的配置：窗口大小与允许延迟为必填语义，
// 其余并发/缓冲参数取保守的有界默认值。
func NewConfig(shards int, windowSize, allowedLateness time.Duration) (Config, error) {
	cfg := Config{
		ShardCount:         shards,
		WindowSize:         windowSize,
		AllowedLateness:    allowedLateness,
		MaxPendingOutputs:  defaultMaxPendingOutputs,
		IngestBuffer:       defaultIngestBuffer,
		CheckpointInterval: defaultCheckpointInterval,
		WatermarkTick:      defaultWatermarkTick,
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// Validate 校验配置的不变量。
func (c Config) Validate() error {
	if c.ShardCount <= 0 {
		return fmt.Errorf("%w: ShardCount must be > 0", ErrInvalidConfig)
	}
	if c.WindowSize <= 0 {
		return fmt.Errorf("%w: WindowSize must be > 0", ErrInvalidConfig)
	}
	if c.AllowedLateness < 0 {
		return fmt.Errorf("%w: AllowedLateness must be >= 0", ErrInvalidConfig)
	}
	if c.MaxPendingOutputs <= 0 {
		return fmt.Errorf("%w: MaxPendingOutputs must be > 0", ErrInvalidConfig)
	}
	if c.IngestBuffer <= 0 {
		return fmt.Errorf("%w: IngestBuffer must be > 0", ErrInvalidConfig)
	}
	if c.CheckpointInterval <= 0 {
		return fmt.Errorf("%w: CheckpointInterval must be > 0", ErrInvalidConfig)
	}
	if c.WatermarkTick <= 0 {
		return fmt.Errorf("%w: WatermarkTick must be > 0", ErrInvalidConfig)
	}
	return nil
}
