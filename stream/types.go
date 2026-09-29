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

// Output 是窗口触发或修正后产出的聚合结果。
//
// 修正语义（面向下游）：
//   - 同一 (窗口, 键) 的输出 ID 不变；Revision 从 1 开始，每次迟到数据修正单调 +1。
//   - Revision 的先后在固定输入下是确定的，与各分片到达先后、并发调度无关。
//   - 下游以 (ID, Revision) 幂等：相同 Revision 重复投递只生效一次；
//     只采用每个 ID 的最大 Revision（最后写入即最新），既不累加也不回退到旧值。
type Output struct {
	ID        string // 确定性输出 ID：窗口 + 键，用于幂等写入
	Revision  int64  // 该 (窗口, 键) 的单调修订号：1=首次触发，>1=迟到数据修正
	Key       string
	Window    Window
	Sum       float64
	Count     int
	Watermark time.Time // 产出（触发或修正）时的水位线
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
