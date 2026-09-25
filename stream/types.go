// Package stream 提供可嵌入式的事件时间流处理运行时。
package stream

import "time"

// Event 是一条待处理的输入事件。
type Event struct {
	ID        string    // 全局唯一事件 ID，用于去重
	Key       string    // 聚合键
	Shard     int       // 来源分片
	Offset    uint64    // 在来源分片中的位点
	EventTime time.Time // 事件时间
	Value     float64   // 参与聚合的数值
}

// Window 表示一个左闭右开的事件时间窗口 [Start, End)。
type Window struct {
	Start time.Time
	End   time.Time
}

// Output 是窗口触发后产出的聚合结果。
type Output struct {
	ID        string // 确定性输出 ID：窗口 + 键，用于幂等写入
	Key       string
	Window    Window
	Sum       float64
	Count     int
	Watermark time.Time // 触发时的水位线
}

// RejectReason 表示事件被拒绝的原因。
type RejectReason int

const (
	RejectNone      RejectReason = iota
	RejectDuplicate              // 重复投递
	RejectLate                   // 超出允许延迟
)

func (r RejectReason) String() string {
	switch r {
	case RejectDuplicate:
		return "duplicate"
	case RejectLate:
		return "late"
	default:
		return "none"
	}
}
