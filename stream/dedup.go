package stream

import "sort"

// DedupStore 记录已处理的事件 ID，保证同一事件不被重复计入聚合。
type DedupStore struct {
	seen map[string]struct{}
}

func NewDedupStore() *DedupStore {
	return &DedupStore{seen: make(map[string]struct{})}
}

// CheckAndMark 若 ID 已存在返回 false，否则记录并返回 true。
func (d *DedupStore) CheckAndMark(id string) bool {
	if _, ok := d.seen[id]; ok {
		return false
	}
	d.seen[id] = struct{}{}
	return true
}

// Snapshot 返回当前去重集合的副本（用于检查点），排序保证序列化确定性。
func (d *DedupStore) Snapshot() []string {
	ids := make([]string, 0, len(d.seen))
	for id := range d.seen {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Restore 从快照恢复去重集合。
func (d *DedupStore) Restore(ids []string) {
	d.seen = make(map[string]struct{}, len(ids))
	for _, id := range ids {
		d.seen[id] = struct{}{}
	}
}
