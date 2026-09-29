package stream

import (
	"sort"
	"time"
)

// DedupEntry 是去重集合中的一条记录：事件 ID 与其事件时间。
// 携带事件时间是为了按水位线做安全垃圾回收（见 CollectBefore）。
type DedupEntry struct {
	ID        string
	EventTime int64 // unix nano
}

// DedupStore 记录已处理的事件 ID，保证同一事件不被重复计入聚合。
// 已关闭窗口对应的 ID 会被安全回收，使长期运行下集合规模保持有界。
type DedupStore struct {
	seen map[string]time.Time
}

func NewDedupStore() *DedupStore {
	return &DedupStore{seen: make(map[string]time.Time)}
}

// CheckAndMark 若 ID 已存在返回 false，否则以其事件时间记录并返回 true。
func (d *DedupStore) CheckAndMark(id string, eventTime time.Time) bool {
	if _, ok := d.seen[id]; ok {
		return false
	}
	d.seen[id] = eventTime
	return true
}

// CollectBefore 按“窗口彻底关闭”安全边界回收去重 ID。
//
// 对于事件时间为 t 的记录，其所属滚动窗口 [start, start+size) 在
// start+size+allowedLateness <= watermark 后才彻底关闭。
// 只有彻底关闭窗口中的 ID 才能删除：
//   - 此前该 ID 的迟到副本仍可能到达并需要被识别为重复（不能提前删）；
//   - 窗口彻底关闭后，该窗口的事件一律按 RejectLate 拒绝，删 ID 不影响任何结果。
//
// 返回被回收的条目数，便于审计与测试。
func (d *DedupStore) CollectBefore(watermark time.Time, size, allowedLateness time.Duration) int {
	if watermark.IsZero() {
		return 0
	}
	removed := 0
	for id, et := range d.seen {
		ns := et.UnixNano()
		sizeNs := size.Nanoseconds()
		start := ns - mod(ns, sizeNs)
		closeAt := time.Unix(0, start+sizeNs+allowedLateness.Nanoseconds()).UTC()
		if !closeAt.After(watermark) {
			delete(d.seen, id)
			removed++
		}
	}
	return removed
}

// Len 返回当前去重集合大小（用于状态规模观测与测试）。
func (d *DedupStore) Len() int { return len(d.seen) }

// Snapshot 返回当前去重集合的副本（用于检查点），按 ID 排序保证序列化确定性。
func (d *DedupStore) Snapshot() []DedupEntry {
	entries := make([]DedupEntry, 0, len(d.seen))
	for id, et := range d.seen {
		entries = append(entries, DedupEntry{ID: id, EventTime: et.UnixNano()})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	return entries
}

// Restore 从快照恢复去重集合。
func (d *DedupStore) Restore(entries []DedupEntry) {
	d.seen = make(map[string]time.Time, len(entries))
	for _, e := range entries {
		d.seen[e.ID] = time.Unix(0, e.EventTime).UTC()
	}
}
