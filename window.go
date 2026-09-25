package streamruntime

import (
	"sort"
	"time"
)

// Window 表示一个事件时间滚动窗口 [Start, End)。
type Window struct {
	Start time.Time
	End   time.Time
}

// Aggregate 是窗口内某个 Key 的聚合结果。
type Aggregate struct {
	Sum   int64
	Count int64
}

// keyAgg 在聚合值之外记录该 Key 是否已经触发过首次输出。
type keyAgg struct {
	agg     Aggregate
	flushed bool
}

// windowEntry 是单个活跃窗口的全部状态。
type windowEntry struct {
	win      Window
	keys     map[string]*keyAgg
	eventIDs map[string]struct{}
}

// WindowState 保存全部活跃窗口的聚合与去重状态。
// 仅由运行时主状态 goroutine 访问，不需要额外同步。
type WindowState struct {
	windowSize time.Duration
	entries    map[int64]*windowEntry
}

// NewWindowState 创建空窗口状态。
func NewWindowState(windowSize time.Duration) *WindowState {
	return &WindowState{
		windowSize: windowSize,
		entries:    make(map[int64]*windowEntry),
	}
}

// AssignWindow 返回事件时间 t 所属的滚动窗口。
func AssignWindow(t time.Time, size time.Duration) Window {
	start := t.Truncate(size)
	return Window{Start: start, End: start.Add(size)}
}

// deadline 返回窗口在给定允许延迟下的最终接收截止时刻：End + allowedLateness。
func (win Window) deadline(allowedLateness time.Duration) time.Time {
	return win.End.Add(allowedLateness)
}

// Apply 把事件计入对应窗口。
// 返回值 kind 为 0 表示接收；否则为 RejectDuplicate / RejectTooLate。
// correctedKey 非空时，表示事件落入已触发窗口，该 Key 的聚合被修正，需要重发。
func (s *WindowState) Apply(e Event, wm time.Time, allowedLateness time.Duration) (kind RejectKind, correctedKey string) {
	win := AssignWindow(e.EventTime, s.windowSize)

	// 迟到判定：水位线（若存在）已越过窗口的延迟截止时刻，则窗口不可再接收事件。
	if !wm.IsZero() && !win.deadline(allowedLateness).After(wm) {
		return RejectTooLate, ""
	}

	entry := s.entries[win.Start.UnixNano()]
	if entry == nil {
		entry = &windowEntry{
			win:      win,
			keys:     make(map[string]*keyAgg),
			eventIDs: make(map[string]struct{}),
		}
		s.entries[win.Start.UnixNano()] = entry
	}

	if _, dup := entry.eventIDs[e.ID]; dup {
		return RejectDuplicate, ""
	}
	entry.eventIDs[e.ID] = struct{}{}

	ka := entry.keys[e.Key]
	if ka == nil {
		ka = &keyAgg{}
		entry.keys[e.Key] = ka
	}
	ka.agg.Sum += e.Value
	ka.agg.Count++

	if ka.flushed {
		return 0, e.Key
	}
	return 0, ""
}

// emitRecord 构造一条输出（Seq 由运行时统一分配，此处留 0）。
func emitRecord(wm time.Time, win Window, key string, agg Aggregate) OutputRecord {
	return OutputRecord{
		Key:         key,
		WindowStart: win.Start,
		WindowEnd:   win.End,
		Watermark:   wm,
		Sum:         agg.Sum,
		Count:       agg.Count,
	}
}

// sortedEntries 返回按窗口起点排序的窗口条目。
func (s *WindowState) sortedEntries() []*windowEntry {
	out := make([]*windowEntry, 0, len(s.entries))
	for _, e := range s.entries {
		out = append(out, e)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].win.Start.Before(out[j].win.Start) })
	return out
}

// sortedKeys 返回条目中按字典序排序的 Key。
func sortedKeys(entry *windowEntry) []string {
	keys := make([]string, 0, len(entry.keys))
	for k := range entry.keys {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// FlushLimited 触发水位线已越过窗口 End、且尚未触发的 Key 聚合，
// 最多生成 max 条输出；被触发的 Key 立即标记 flushed。
// 顺序确定：按窗口起点、再按 Key 字典序。
func (s *WindowState) FlushLimited(wm time.Time, max int) []OutputRecord {
	if max <= 0 {
		return nil
	}
	out := make([]OutputRecord, 0, max)
	for _, entry := range s.sortedEntries() {
		if wm.Before(entry.win.End) {
			continue
		}
		for _, k := range sortedKeys(entry) {
			ka := entry.keys[k]
			if ka.flushed {
				continue
			}
			ka.flushed = true
			out = append(out, emitRecord(wm, entry.win, k, ka.agg))
			if len(out) >= max {
				return out
			}
		}
	}
	return out
}

// EmitKey 重新下发指定窗口单个 Key 的当前聚合（用于迟到事件导致的修正）。
func (s *WindowState) EmitKey(win Window, key string, wm time.Time) (OutputRecord, bool) {
	entry := s.entries[win.Start.UnixNano()]
	if entry == nil {
		return OutputRecord{}, false
	}
	ka := entry.keys[key]
	if ka == nil || !ka.flushed {
		return OutputRecord{}, false
	}
	return emitRecord(wm, entry.win, key, ka.agg), true
}

// Purge 清理同时满足以下条件的窗口：
//  1. 水位线已越过 End+AllowedLateness（不可能再接收迟到事件）；且
//  2. 该窗口所有 Key 都已触发（不存在"窗口已删但输出从未生成"的静默丢失）。
func (s *WindowState) Purge(wm time.Time, allowedLateness time.Duration) []Window {
	var purged []Window
	for startNano, entry := range s.entries {
		allFlushed := true
		for _, ka := range entry.keys {
			if !ka.flushed {
				allFlushed = false
				break
			}
		}
		if allFlushed && !wm.Before(entry.win.deadline(allowedLateness)) {
			purged = append(purged, entry.win)
			delete(s.entries, startNano)
		}
	}
	sort.Slice(purged, func(i, j int) bool { return purged[i].Start.Before(purged[j].Start) })
	return purged
}

// ActiveWindowCount 返回当前持有的窗口数量，用于观测内存边界。
func (s *WindowState) ActiveWindowCount() int { return len(s.entries) }

// FlushAll 触发所有尚未触发的 Key 聚合，不做水位线门槛判断（仅用于全部输入结束时的收尾）。
func (s *WindowState) FlushAll(wm time.Time) []OutputRecord {
	return s.flushAll(wm, 0)
}

// FlushAllLimited 同 FlushAll，但最多生成 max 条，未触发的 Key 保留原状。
func (s *WindowState) FlushAllLimited(wm time.Time, max int) []OutputRecord {
	if max <= 0 {
		return nil
	}
	return s.flushAll(wm, max)
}

func (s *WindowState) flushAll(wm time.Time, max int) []OutputRecord {
	out := make([]OutputRecord, 0)
	for _, entry := range s.sortedEntries() {
		for _, k := range sortedKeys(entry) {
			ka := entry.keys[k]
			if ka.flushed {
				continue
			}
			ka.flushed = true
			out = append(out, emitRecord(wm, entry.win, k, ka.agg))
			if max > 0 && len(out) >= max {
				return out
			}
		}
	}
	return out
}

// HasUnflushed 返回是否还有未触发的 Key 聚合。
func (s *WindowState) HasUnflushed() bool {
	for _, entry := range s.entries {
		for _, ka := range entry.keys {
			if !ka.flushed {
				return true
			}
		}
	}
	return false
}

// PurgeAll 清空全部窗口状态（仅用于所有输出已确认、运行正常结束时）。
func (s *WindowState) PurgeAll() int {
	n := len(s.entries)
	s.entries = make(map[int64]*windowEntry)
	return n
}

// Snapshot 导出全部窗口状态用于检查点。
func (s *WindowState) Snapshot() []windowSnapshot {
	out := make([]windowSnapshot, 0, len(s.entries))
	for _, entry := range s.sortedEntries() {
		sn := windowSnapshot{
			Start:      entry.win.Start,
			End:        entry.win.End,
			KeyAggs:    make(map[string]Aggregate, len(entry.keys)),
			KeyFlushed: make(map[string]bool, len(entry.keys)),
			EventIDs:   make(map[string]struct{}, len(entry.eventIDs)),
		}
		for k, ka := range entry.keys {
			sn.KeyAggs[k] = ka.agg
			sn.KeyFlushed[k] = ka.flushed
		}
		for id := range entry.eventIDs {
			sn.EventIDs[id] = struct{}{}
		}
		out = append(out, sn)
	}
	return out
}

// LoadSnapshot 从检查点恢复窗口状态，覆盖当前全部内容。
func (s *WindowState) LoadSnapshot(snaps []windowSnapshot) {
	s.entries = make(map[int64]*windowEntry, len(snaps))
	for _, sn := range snaps {
		entry := &windowEntry{
			win:      Window{Start: sn.Start, End: sn.End},
			keys:     make(map[string]*keyAgg, len(sn.KeyAggs)),
			eventIDs: make(map[string]struct{}, len(sn.EventIDs)),
		}
		for k, agg := range sn.KeyAggs {
			entry.keys[k] = &keyAgg{agg: agg, flushed: sn.KeyFlushed[k]}
		}
		for id := range sn.EventIDs {
			entry.eventIDs[id] = struct{}{}
		}
		s.entries[sn.Start.UnixNano()] = entry
	}
}
