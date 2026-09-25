package stream

import (
	"fmt"
	"sort"
	"time"
)

// WindowState 保存单个 (窗口, 键) 的聚合中间状态。
type WindowState struct {
	Key    string
	Window Window
	Sum    float64
	Count  int
}

// WindowManager 管理滚动事件时间窗口的聚合状态与触发。
type WindowManager struct {
	size            time.Duration
	allowedLateness time.Duration
	states          map[string]*WindowState
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

// Add 将事件计入对应窗口；事件时间 <= 水位线-允许延迟 视为迟到，拒绝并返回 RejectLate。
func (m *WindowManager) Add(ev Event, watermark time.Time) RejectReason {
	if !watermark.IsZero() && !ev.EventTime.After(watermark.Add(-m.allowedLateness)) {
		return RejectLate
	}
	w := m.Assign(ev.EventTime)
	k := stateKey(w, ev.Key)
	st, ok := m.states[k]
	if !ok {
		st = &WindowState{Key: ev.Key, Window: w}
		m.states[k] = st
	}
	st.Sum += ev.Value
	st.Count++
	return RejectNone
}

// FireReady 触发所有 End+允许延迟 <= 水位线 的窗口，返回产出结果并清除对应状态。
// 输出按 (窗口起点, 键) 排序，保证多次运行结果确定。
func (m *WindowManager) FireReady(watermark time.Time) []Output {
	var out []Output
	for k, st := range m.states {
		if !st.Window.End.Add(m.allowedLateness).After(watermark) {
			out = append(out, Output{
				ID:        fmt.Sprintf("%d-%d/%s", st.Window.Start.UnixNano(), st.Window.End.UnixNano(), st.Key),
				Key:       st.Key,
				Window:    st.Window,
				Sum:       st.Sum,
				Count:     st.Count,
				Watermark: watermark,
			})
			delete(m.states, k)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Window.Start.Equal(out[j].Window.Start) {
			return out[i].Window.Start.Before(out[j].Window.Start)
		}
		return out[i].Key < out[j].Key
	})
	return out
}

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

// Restore 从快照恢复窗口状态。
func (m *WindowManager) Restore(states []WindowState) {
	m.states = make(map[string]*WindowState, len(states))
	for i := range states {
		st := states[i]
		m.states[stateKey(st.Window, st.Key)] = &st
	}
}
