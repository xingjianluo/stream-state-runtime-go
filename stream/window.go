package stream

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// WindowState 保存单个 (窗口, 键) 的聚合中间状态。
type WindowState struct {
	Key    string
	Window Window
	Sum    float64
	Count  int
	// Fired 表示窗口是否已首次触发。触发后状态仍保留到迟到容忍期结束，
	// 期间到达的迟到数据会计入并产生更高 Revision 的修正结果。
	Fired bool
	// Revision 为已产出的结果版本：首次触发为 1，每次迟到修正 +1。
	Revision int
}

// WindowManager 管理滚动事件时间窗口的聚合状态、触发、修正与到期清理。
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

// outputID 确定性生成 (窗口, 键) 的输出 ID；首次触发与各次修正共用同一 ID。
func outputID(w Window, key string) string {
	return fmt.Sprintf("%d-%d/%s", w.Start.UnixNano(), w.End.UnixNano(), key)
}

// parseOutputID 解析 outputID，返回窗口起点 UnixNano 与键。
func parseOutputID(id string) (int64, string, bool) {
	slash := strings.IndexByte(id, '/')
	if slash <= 0 {
		return 0, "", false
	}
	dash := strings.IndexByte(id[:slash], '-')
	if dash <= 0 {
		return 0, "", false
	}
	start, err := strconv.ParseInt(id[:dash], 10, 64)
	if err != nil {
		return 0, "", false
	}
	return start, id[slash+1:], true
}

// HasOutputID 返回某输出 ID 对应的窗口状态是否仍保留（未到期清理）。
func (m *WindowManager) HasOutputID(id string) bool {
	start, key, ok := parseOutputID(id)
	if !ok {
		return false
	}
	_, ok = m.states[stateKey(Window{Start: time.Unix(0, start).UTC()}, key)]
	return ok
}

// Add 将事件计入对应窗口。
// 事件时间 <= 水位线-允许延迟 视为不可接受的迟到，拒绝并返回 RejectLate。
// 第二个返回值为 true 表示该事件改变了一个已触发窗口，调用方必须补发修正结果。
func (m *WindowManager) Add(ev Event, watermark time.Time) (RejectReason, bool) {
	if !watermark.IsZero() && !ev.EventTime.After(watermark.Add(-m.allowedLateness)) {
		return RejectLate, false
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
	return RejectNone, st.Fired
}

func (m *WindowManager) toOutput(st *WindowState, watermark time.Time) Output {
	return Output{
		ID:        outputID(st.Window, st.Key),
		Key:       st.Key,
		Window:    st.Window,
		Sum:       st.Sum,
		Count:     st.Count,
		Revision:  st.Revision,
		Watermark: watermark,
	}
}

// sortOutputs 按 (窗口起点, 键) 排序，保证产出顺序确定。
func sortOutputs(out []Output) {
	sort.Slice(out, func(i, j int) bool {
		if !out[i].Window.Start.Equal(out[j].Window.Start) {
			return out[i].Window.Start.Before(out[j].Window.Start)
		}
		return out[i].Key < out[j].Key
	})
}

// EmitReady 首次触发所有 End <= 水位线 且尚未触发的窗口：标记 Fired、版本置 1，
// 但保留状态以接收迟到数据。输出按 (窗口起点, 键) 排序，确定可重放。
func (m *WindowManager) EmitReady(watermark time.Time) []Output {
	var out []Output
	for _, st := range m.states {
		if !st.Fired && !st.Window.End.After(watermark) {
			st.Fired = true
			st.Revision = 1
			out = append(out, m.toOutput(st, watermark))
		}
	}
	sortOutputs(out)
	return out
}

// EmitCorrection 为已触发窗口产出下一版本的修正结果（Revision+1）。
// 窗口不存在或尚未触发时第二个返回值为 false。
func (m *WindowManager) EmitCorrection(w Window, key string, watermark time.Time) (Output, bool) {
	st, ok := m.states[stateKey(w, key)]
	if !ok || !st.Fired {
		return Output{}, false
	}
	st.Revision++
	return m.toOutput(st, watermark), true
}

// PurgeExpired 清理所有 End+允许延迟 <= 水位线 的窗口状态。
// 到达该线的窗口绝不可能再被迟到数据改变，清理后状态规模与运行时长无关。
// 返回被清理的状态（调用方可据此清理与之配套的进度记录），顺序确定。
func (m *WindowManager) PurgeExpired(watermark time.Time) []WindowState {
	var purged []WindowState
	for k, st := range m.states {
		if !st.Window.End.Add(m.allowedLateness).After(watermark) {
			purged = append(purged, *st)
			delete(m.states, k)
		}
	}
	sort.Slice(purged, func(i, j int) bool {
		if !purged[i].Window.Start.Equal(purged[j].Window.Start) {
			return purged[i].Window.Start.Before(purged[j].Window.Start)
		}
		return purged[i].Key < purged[j].Key
	})
	return purged
}

// Len 返回当前保留的窗口状态数（未到期清理的 (窗口,键) 条目数）。
func (m *WindowManager) Len() int { return len(m.states) }

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
