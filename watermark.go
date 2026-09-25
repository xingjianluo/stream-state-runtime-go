package streamruntime

import (
	"sync"
	"time"
)

// Watermark 是单调只增的事件时间水位线，并发安全。
// 零值时间（time.Time{}）被视为 "-∞"：任何非零时间都能越过它。
type Watermark struct {
	mu    sync.Mutex
	value time.Time
	set   bool
}

// NewWatermark 创建初始水位线。
func NewWatermark() *Watermark { return &Watermark{} }

// Load 从已有快照恢复初始水位线。
func (w *Watermark) Load(t time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.value = t
	w.set = !t.IsZero()
}

// Value 返回当前水位线；从未推进时返回零值时间。
func (w *Watermark) Value() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.value
}

// Advance 尝试把水位线推进到 t。
// 只有当 t 严格晚于当前水位线时才推进，返回新水位线和 true；
// 任何回退（包括重复推进到同一时刻）都被拒绝，返回当前水位线和 false。
func (w *Watermark) Advance(t time.Time) (time.Time, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.set || t.After(w.value) {
		w.value = t
		w.set = true
		return t, true
	}
	return w.value, false
}
