package stream

import "time"

// Config 为运行时配置项。
type Config struct {
	WindowSize      time.Duration // 滚动窗口大小
	AllowedLateness time.Duration // 允许延迟：事件时间 <= 水位线-允许延迟 的事件被拒绝
	FetchBatch      int           // 每次拉取的最大事件数
	InFlightLimit   int           // 在途事件上限（背压）：已拉取未处理的事件总数不超过该值
	CheckpointEvery int           // 每处理多少事件做一次检查点

	CheckpointStore CheckpointStore
	// Logger 可选：输出关键事件时间、水位线、检查点与背压判定依据，便于审计与测试。
	Logger func(format string, args ...any)
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.WindowSize <= 0 {
		out.WindowSize = time.Minute
	}
	if out.FetchBatch <= 0 {
		out.FetchBatch = 16
	}
	if out.InFlightLimit <= 0 {
		out.InFlightLimit = 64
	}
	if out.CheckpointEvery <= 0 {
		out.CheckpointEvery = 32
	}
	return out
}

func (c *Config) logf(format string, args ...any) {
	if c.Logger != nil {
		c.Logger(format, args...)
	}
}
