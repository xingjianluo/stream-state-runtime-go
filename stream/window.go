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
// 这里有两条不同的边界，不可混用：
//
//	事件级迟到线（Add 的接收/拒绝判定）：事件时间 + AllowedLateness <= 水位线。
//	  —— 严格按配置的 AllowedLateness 度量“落后水位线多少”，与窗口大小无关，
//	     不允许被窗口跨度放大。越线事件即使所属窗口尚未彻底关闭也一律拒绝。
//	窗口关闭线（FireReady 的状态清理）：窗口 End + AllowedLateness <= 水位线。
//
// 生命周期（按组合水位线）：
//   - End <= 水位线：首次触发，输出 Revision=1，但状态保留以接收允许延迟内的迟到数据。
//   - End < ... <= End+AllowedLateness：仅“事件时间+允许延迟仍晚于水位线”的迟到数据
//     才并入已触发状态，产出 Revision 递增的修正；更早的迟到数据按事件级迟到线拒绝。
//   - End+AllowedLateness <= 水位线：窗口彻底关闭并删除状态；此时属于该窗口的事件其
//     事件时间+允许延迟必然也不晚于水位线，已在事件级迟到线被拒绝。
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
// 事件级迟到判定（严格按配置，不被窗口大小放大）：当
//
//	事件时间 + AllowedLateness <= 判定水位线
//
// 即该事件落后水位线已达到/超过允许延迟时，返回 RejectLate 且不修改任何状态，
// 即使其所属窗口尚未彻底关闭（窗口关闭线 End+AllowedLateness 更晚）。
// 否则并入状态：
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
// 并彻底清理 End+允许延迟 <= 水位线 的窗口（窗口级关闭线，仅用于状态回收，
// 不参与事件的迟到判定——后者见 Add 的事件级迟到线）。
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
