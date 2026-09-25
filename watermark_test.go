package streamruntime

import (
	"testing"
	"time"
)

func TestWatermarkMonotonic(t *testing.T) {
	w := NewWatermark()
	base := time.Unix(1000, 0).UTC()

	if got, ok := w.Advance(base); !ok || !got.Equal(base) {
		t.Fatalf("first advance: got=%v ok=%v", got, ok)
	}
	// 回退必须被拒绝。
	if _, ok := w.Advance(base.Add(-time.Second)); ok {
		t.Fatal("watermark must not move backwards")
	}
	// 重复推进到同一时刻也不算推进。
	if _, ok := w.Advance(base); ok {
		t.Fatal("equal timestamp must not advance watermark")
	}
	if got, ok := w.Advance(base.Add(5 * time.Second)); !ok || !got.Equal(base.Add(5*time.Second)) {
		t.Fatalf("forward advance failed: got=%v ok=%v", got, ok)
	}
	if !w.Value().Equal(base.Add(5 * time.Second)) {
		t.Fatalf("unexpected value: %v", w.Value())
	}
}
