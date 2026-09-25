package streamruntime

import (
	"testing"
	"time"
)

func sec(n int64) time.Time { return time.Unix(n, 0).UTC() }

func TestWindowAggregateOutOfOrderAndDedup(t *testing.T) {
	ws := NewWindowState(time.Minute)
	wm := sec(0) // 尚未推进的水位线：不拒绝任何事件

	ev := func(id string, ts int64, key string, v int64) Event {
		return Event{ID: id, Shard: 0, Offset: int64(len(id)), Key: key, EventTime: sec(ts), Value: v}
	}

	// 同一窗口 [00:00,01:00) 乱序进入两条，再加一条重复。
	if k, _ := ws.Apply(ev("a", 30, "k1", 10), wm, 30*time.Second); k != 0 {
		t.Fatalf("accept a, got kind=%v", k)
	}
	if k, _ := ws.Apply(ev("b", 10, "k1", 5), wm, 30*time.Second); k != 0 {
		t.Fatalf("accept b, got kind=%v", k)
	}
	if k, _ := ws.Apply(ev("a", 30, "k1", 10), wm, 30*time.Second); k != RejectDuplicate {
		t.Fatalf("duplicate must be rejected, got kind=%v", k)
	}

	// 水位线到 01:00：窗口首次触发。
	out := ws.FlushLimited(sec(60), 10)
	if len(out) != 1 || out[0].Count != 2 || out[0].Sum != 15 {
		t.Fatalf("unexpected first flush: %+v", out)
	}
	t.Logf("首次触发 window=%s..%s count=%d sum=%d wm=%s",
		out[0].WindowStart.Format("15:04:05"), out[0].WindowEnd.Format("15:04:05"),
		out[0].Count, out[0].Sum, sec(60).Format("15:04:05"))

	// FlushLimited 必须幂等：再次推进不应重复触发。
	if out2 := ws.FlushLimited(sec(70), 10); len(out2) != 0 {
		t.Fatalf("second flush must be empty, got %+v", out2)
	}

	// 允许延迟 30s：事件时间 00:45 在 wm=01:10 仍可接收（deadline=01:30），且应触发修正重发。
	if k, key := ws.Apply(ev("c", 45, "k1", 7), sec(70), 30*time.Second); k != 0 || key != "k1" {
		t.Fatalf("on-time-but-late event accepted and corrected, kind=%v key=%q", k, key)
	}
	rec, ok := ws.EmitKey(AssignWindow(sec(45), time.Minute), "k1", sec(70))
	if !ok || rec.Count != 3 || rec.Sum != 22 {
		t.Fatalf("corrected emit: ok=%v %+v", ok, rec)
	}
	t.Logf("迟到修正 window count=%d sum=%d", rec.Count, rec.Sum)

	// wm=01:31 超过 deadline=01:30：事件时间 00:50 被拒绝，且不计入。
	if k, _ := ws.Apply(ev("d", 50, "k1", 100), sec(91), 30*time.Second); k != RejectTooLate {
		t.Fatalf("too-late event must be rejected, got %v", k)
	}

	// Purge：wm=01:30 时窗口可清理。
	purged := ws.Purge(sec(90), 30*time.Second)
	if len(purged) != 1 || ws.ActiveWindowCount() != 0 {
		t.Fatalf("purge failed: %+v remain=%d", purged, ws.ActiveWindowCount())
	}
}

func TestAssignWindow(t *testing.T) {
	win := AssignWindow(sec(90), time.Minute)
	if !win.Start.Equal(sec(60)) || !win.End.Equal(sec(120)) {
		t.Fatalf("unexpected window: %+v", win)
	}
}
