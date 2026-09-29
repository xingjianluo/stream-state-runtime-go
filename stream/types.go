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
// 下游语义（upsert / latest-wins）：
//   - ID 对同一 (窗口, 键) 恒定，首次触发与之后每次迟到修正的 ID 相同；
//   - Revision 从 1 开始单调递增，仅在结果真正变化时 +1；下游应保留
//     同一 ID 中 Revision 最大的一份，旧版本（含重试重发）必须被忽略；
//   - 同一次修正可能因重试被重复投递，相同 (ID, Revision) 幂等，不得累加。
type Output struct {
	ID        string // 确定性输出 ID：窗口 + 键，用于幂等/更新写入
	Key       string
	Window    Window
	Sum       float64
	Count     int
	Revision  int       // 结果版本：首次触发为 1，每次迟到数据修正 +1
	Watermark time.Time // 本次产出时的水位线
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
