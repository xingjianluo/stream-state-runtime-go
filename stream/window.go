package stream

import (
	"fmt"
	"sort"
	"time"
)

// WindowState 保存单个 (窗口, 键) 的聚合中间状态。
type WindowState struct {
	Key      string
	Window   Window
	Sum      float64
	Count    int
	Revision int64 // 该 (窗口, 键) 已产出的修订号：0=尚未首次触发，>0=已触发/修正次数
	Fired    bool  // 是否已首次触发（之后迟到数据产生修正而非首次输出）
}

// WindowManager 管理滚动事件时间窗口的聚合状态、触发与修正。
//
// 这里有两条相互独立的边界，切勿混用：
//
//  1. 接收边界（按“事件时间 + 允许延迟”判定，作用于每条输入）：
//     当 ev.EventTime+AllowedLateness <= 水位线 时，事件按 RejectLate 拒绝，
//     不修改任何状态、不产生任何输出——即使其所属窗口尚未彻底关闭也一样。
//     允许延迟的长度就是 AllowedLateness 本身，不会被窗口大小放大：
//     落后水位线超过允许延迟的输入稳定拒绝；仍在允许延迟内的迟到数据并入并修正。
//  2. 回收边界（按“窗口 End + 允许延迟”判定，作用于状态清理）：
//     窗口 [Start,End) 内最晚的事件时间严格小于 End，因此当
//     End+AllowedLateness <= 水位线 时，该窗口的任何事件都必然越过接收边界，
//     此时才彻底关闭并删除窗口状态、回收相关去重 ID。
//
// 生命周期（按组合水位线）：
//   - End <= 水位线：首次触发，输出 Revision=1，但状态保留以接收允许延迟内的迟到数据。
//   - 首次触发之后：只有满足接收边界的迟到数据才并入，产出 Revision 递增的修正；
//     越过接收边界的迟到数据一律拒绝，哪怕窗口状态此刻仍保留着。
//   - End+AllowedLateness <= 水位线：窗口彻底关闭并删除状态；之后属于该窗口的事件拒绝。
type WindowManager struct {
	size            time.Duration
	allowedLateness time.Duration
	states          map[string]*WindowState
	corrections     []Output // Add 并入的修正，等待下一次 FireReady 冲刷
}

func NewWindowManager(size, allowedLateness time.Duration) *WindowManager {
	return &WindowManager{
		size:            size,
		allowedLateness: allowedLateness,
		states:          make(map[string]*WindowState),
	}
}

// Assign 返回事件时间所属的滚动窗口（按 Unix 纳秒对齐）。
func (m *WindowManager) Assign(ts time.Time) Window {
	ns := ts.UnixNano()
	size := m.size.Nanoseconds()
	start := ns - mod(ns, size)
	return Window{Start: time.Unix(0, start).UTC(), End: time.Unix(0, start+size).UTC()}
}

func mod(a, b int64) int64 {
	r := a % b
	if r < 0 {
		r += b
	}
	return r
}

func stateKey(w Window, key string) string {
	return fmt.Sprintf("%d/%s", w.Start.UnixNano(), key)
}

func outputID(st *WindowState) string {
	return fmt.Sprintf("%d-%d/%s", st.Window.Start.UnixNano(), st.Window.End.UnixNano(), st.Key)
}

func makeOutput(st *WindowState, watermark time.Time) Output {
	return Output{
		ID:        outputID(st),
		Revision:  st.Revision,
		Key:       st.Key,
		Window:    st.Window,
		Sum:       st.Sum,
		Count:     st.Count,
		Watermark: watermark,
	}
}

// Add 将事件计入对应窗口。
//
// 接收边界（与窗口大小无关）：事件时间满足 ev.EventTime+AllowedLateness <= 判定水位线
// 时，按 RejectLate 拒绝，且不修改任何状态、不产生修正。注意这是按事件时间判定，
// 不是按窗口 End 判定——事件可能落在窗口前部，此时窗口虽未彻底关闭（窗口状态仍保留、
// 去重 ID 仍受保护），该事件也必须丢弃。
//
// 未越过接收边界的事件并入状态：
//   - 窗口尚未首次触发：只更新中间聚合，暂不产出（等 FireReady 首次触发）。
//   - 窗口已首次触发：产生一条 Revision 递增的修正结果，由下一次 FireReady 冲刷。
func (m *WindowManager) Add(ev Event, watermark time.Time) RejectReason {
	w := m.Assign(ev.EventTime)
	if !watermark.IsZero() && !ev.EventTime.Add(m.allowedLateness).After(watermark) {
		return RejectLate
	}
	k := stateKey(w, ev.Key)
	st, ok := m.states[k]
	if !ok {
		st = &WindowState{Key: ev.Key, Window: w}
		m.states[k] = st
	}
	st.Sum += ev.Value
	st.Count++
	if st.Fired {
		st.Revision++
		m.corrections = append(m.corrections, makeOutput(st, watermark))
	}
	return RejectNone
}

// FireReady 处理水位线推进：返回首次触发输出与自上次调用以来并入的修正输出，
// 并按回收边界清理 End+允许延迟 <= 水位线 的窗口状态。
// 注意：这里的“窗口 End+允许延迟”只用于状态回收；单条事件是否接收由 Add 按
// “事件时间+允许延迟”判定，两条边界互不替代。
// 输出按 (窗口起点, 键, Revision) 排序，保证多次运行、多分片并发下结果确定。
func (m *WindowManager) FireReady(watermark time.Time) []Output {
	out := m.corrections
	m.corrections = nil
	for k, st := range m.states {
		if !st.Fired && !st.Window.End.After(watermark) {
			st.Fired = true
			st.Revision = 1
			out = append(out, makeOutput(st, watermark))
		}
		if st.Fired && !st.Window.End.Add(m.allowedLateness).After(watermark) {
			delete(m.states, k) // 允许延迟用尽：窗口彻底关闭，状态清理
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Window.Start.Equal(out[j].Window.Start) {
			return out[i].Window.Start.Before(out[j].Window.Start)
		}
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Revision < out[j].Revision
	})
	return out
}

// ActiveWindows 返回当前仍在内存中的 (窗口, 键) 状态数（用于状态规模观测与测试）。
func (m *WindowManager) ActiveWindows() int { return len(m.states) }

// Snapshot 返回全部窗口状态副本（用于检查点），排序保证序列化确定性。
func (m *WindowManager) Snapshot() []WindowState {
	states := make([]WindowState, 0, len(m.states))
	for _, st := range m.states {
		states = append(states, *st)
	}
	sort.Slice(states, func(i, j int) bool {
		if !states[i].Window.Start.Equal(states[j].Window.Start) {
			return states[i].Window.Start.Before(states[j].Window.Start)
		}
		return states[i].Key < states[j].Key
	})
	return states
}

// Restore 从快照恢复窗口状态；未冲刷的修正不从此处重放（由 Runtime 的 pending 恢复）。
func (m *WindowManager) Restore(states []WindowState) {
	m.states = make(map[string]*WindowState, len(states))
	m.corrections = nil
	for i := range states {
		st := states[i]
		m.states[stateKey(st.Window, st.Key)] = &st
	}
}
