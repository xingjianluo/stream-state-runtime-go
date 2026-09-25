package stream

import "time"

// WatermarkTracker 跟踪多个分片的水位线，组合水位线只能单调推进。
type WatermarkTracker struct {
	perShard map[int]time.Time
	combined time.Time
}

func NewWatermarkTracker(shards int) *WatermarkTracker {
	t := &WatermarkTracker{perShard: make(map[int]time.Time, shards)}
	for i := 0; i < shards; i++ {
		t.perShard[i] = time.Time{}
	}
	return t
}

// Advance 推进某分片水位线，返回推进后的组合水位线（取各分片最小值，单调不减）。
// 单分片水位线回退会被忽略，保证单调性。
func (t *WatermarkTracker) Advance(shard int, wm time.Time) time.Time {
	if cur, ok := t.perShard[shard]; ok && wm.Before(cur) {
		return t.combined // 忽略回退
	}
	t.perShard[shard] = wm
	min := t.perShard[shard]
	for _, v := range t.perShard {
		if v.Before(min) {
			min = v
		}
	}
	if min.After(t.combined) {
		t.combined = min
	}
	return t.combined
}

// Combined 返回当前组合水位线。
func (t *WatermarkTracker) Combined() time.Time { return t.combined }

// AdvanceAll 将所有分片水位线推进到 wm（用于结束语义），返回组合水位线。
func (t *WatermarkTracker) AdvanceAll(wm time.Time) time.Time {
	for shard := range t.perShard {
		t.perShard[shard] = wm
	}
	if wm.After(t.combined) {
		t.combined = wm
	}
	return t.combined
}

// Restore 从检查点恢复：所有分片与组合水位线均设为 wm。
func (t *WatermarkTracker) Restore(wm time.Time) {
	for shard := range t.perShard {
		t.perShard[shard] = wm
	}
	t.combined = wm
}
