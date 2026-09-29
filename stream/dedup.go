package stream

import (
	"sort"
	"time"
)

// DedupStore 记录已处理的事件 ID，保证同一事件不被重复计入聚合。
// 每条记录同时保存其所属窗口的"结果不可变时刻"（窗口 End + 允许延迟），
// 以便在安全边界外回收，使状态规模长期稳定。
type DedupStore struct {
	// seen[id] = 该事件所属窗口的清理时刻（window.End + AllowedLateness）。
	// 用 time.Time 而非 UnixNano 存储：结束语义使用远未来时间（9999 年），
	// 其 UnixNano 会溢出，time.Time 之间的比较不受影响。
	seen map[string]time.Time
}

func NewDedupStore() *DedupStore {
	return &DedupStore{seen: make(map[string]time.Time)}
}

// CheckAndMark 若 ID 已存在返回 false，否则记录 (ID, 所属窗口清理时刻) 并返回 true。
func (d *DedupStore) CheckAndMark(id string, windowPurgeAt time.Time) bool {
	if _, ok := d.seen[id]; ok {
		return false
	}
	d.seen[id] = windowPurgeAt
	return true
}

// GCExpired 删除所属窗口已过清理线（End+允许延迟 <= 水位线）的 ID，返回删除条数。
//
// 安全边界：只要窗口结果还可能被迟到数据改变，该窗口输入的 ID 就必须保留，
// 重复投递仍被识别为 duplicate；窗口一旦过线，其结果永远不可变，此时到达的
// 重复事件只会被判定为不可接受的迟到，删除其 ID 不会造成重复计入或算错。
func (d *DedupStore) GCExpired(watermark time.Time) int {
	if watermark.IsZero() {
		return 0
	}
	n := 0
	for id, purgeAt := range d.seen {
		if !purgeAt.After(watermark) {
			delete(d.seen, id)
			n++
		}
	}
	return n
}

// Len 返回当前保留的去重 ID 数。
func (d *DedupStore) Len() int { return len(d.seen) }

// Snapshot 返回当前去重集合的副本（用于检查点），排序保证序列化确定性。
func (d *DedupStore) Snapshot() []DedupEntry {
	ids := make([]string, 0, len(d.seen))
	for id := range d.seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]DedupEntry, 0, len(ids))
	for _, id := range ids {
		out = append(out, DedupEntry{ID: id, PurgeAt: d.seen[id].UnixNano()})
	}
	return out
}

// Restore 从快照恢复去重集合。
func (d *DedupStore) Restore(entries []DedupEntry) {
	d.seen = make(map[string]time.Time, len(entries))
	for _, e := range entries {
		d.seen[e.ID] = time.Unix(0, e.PurgeAt).UTC()
	}
}
