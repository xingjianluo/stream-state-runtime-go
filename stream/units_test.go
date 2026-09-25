package stream

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func ts(sec int) time.Time { return time.Unix(int64(sec), 0).UTC() }

func TestWatermarkMonotonic(t *testing.T) {
	tr := NewWatermarkTracker(2)
	if got := tr.Advance(0, ts(10)); !got.IsZero() {
		t.Fatalf("组合水位线应受最慢分片约束, got %s", got)
	}
	if got := tr.Advance(1, ts(5)); got != ts(5) {
		t.Fatalf("combined=%s, want %s", got, ts(5))
	}
	// 分片水位线回退必须被忽略
	if got := tr.Advance(0, ts(3)); got != ts(5) {
		t.Fatalf("回退后 combined=%s, want %s", got, ts(5))
	}
	if got := tr.Advance(0, ts(20)); got != ts(5) {
		t.Fatalf("combined=%s, want %s (shard1 仍为 5)", got, ts(5))
	}
	if got := tr.Advance(1, ts(8)); got != ts(8) {
		t.Fatalf("combined=%s, want %s", got, ts(8))
	}
	t.Logf("watermark 单调性验证通过: combined=%s", tr.Combined())
}

func TestWindowAssignAndLateReject(t *testing.T) {
	m := NewWindowManager(10*time.Second, 5*time.Second)
	w := m.Assign(ts(17))
	if w.Start != ts(10) || w.End != ts(20) {
		t.Fatalf("窗口分配错误: %+v", w)
	}
	// 水位线 12, 允许延迟 5 => 事件时间 <= 7 被拒绝
	wm := ts(12)
	if r := m.Add(Event{ID: "a", Key: "k", EventTime: ts(8), Value: 1}, wm); r != RejectNone {
		t.Fatalf("事件应被接收, got %s", r)
	}
	if r := m.Add(Event{ID: "b", Key: "k", EventTime: ts(7), Value: 1}, wm); r != RejectLate {
		t.Fatalf("迟到事件应被拒绝, got %s", r)
	}
	t.Logf("late 拒绝判定: eventTime=%s watermark=%s allowedLateness=5s => %s", ts(7), wm, RejectLate)
}

func TestWindowFireReady(t *testing.T) {
	m := NewWindowManager(10*time.Second, 5*time.Second)
	m.Add(Event{ID: "a", Key: "x", EventTime: ts(1), Value: 2}, ts(1))
	m.Add(Event{ID: "b", Key: "x", EventTime: ts(2), Value: 3}, ts(2))
	m.Add(Event{ID: "c", Key: "y", EventTime: ts(11), Value: 5}, ts(11))
	// 水位线 14：窗口[0,10) 触发线为 15，未就绪
	if outs := m.FireReady(ts(14)); len(outs) != 0 {
		t.Fatalf("不应有窗口触发, got %d", len(outs))
	}
	// 水位线 15：窗口[0,10) 触发
	outs := m.FireReady(ts(15))
	if len(outs) != 1 || outs[0].Key != "x" || outs[0].Sum != 5 || outs[0].Count != 2 {
		t.Fatalf("触发结果错误: %+v", outs)
	}
	t.Logf("fire: window=[%s,%s) key=x sum=%v count=%d watermark=%s",
		outs[0].Window.Start, outs[0].Window.End, outs[0].Sum, outs[0].Count, outs[0].Watermark)
	// 已触发窗口状态必须清除，重复触发不得再次产出
	if outs := m.FireReady(ts(100)); len(outs) != 1 || outs[0].Key != "y" {
		t.Fatalf("剩余窗口触发错误: %+v", outs)
	}
	if outs := m.FireReady(ts(1000)); len(outs) != 0 {
		t.Fatalf("状态未清除，重复触发: %+v", outs)
	}
}

func TestDedup(t *testing.T) {
	d := NewDedupStore()
	if !d.CheckAndMark("e1") {
		t.Fatal("首次应接收")
	}
	if d.CheckAndMark("e1") {
		t.Fatal("重复应拒绝")
	}
	snap := d.Snapshot()
	d2 := NewDedupStore()
	d2.Restore(snap)
	if d2.CheckAndMark("e1") {
		t.Fatal("恢复后重复应拒绝")
	}
}

func TestCheckpointCorruptAndIncompatible(t *testing.T) {
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}

	// 损坏：写入垃圾数据
	if err := os.WriteFile(store.Path, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, _, err := store.Load(t.Context())
	if !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("损坏检查点应返回 ErrCorruptCheckpoint, got %v", err)
	}
	t.Logf("损坏检查点拒绝原因: %v", err)

	// 不兼容版本：构造合法封装但版本号不同
	good := Checkpoint{Version: FormatVersion, Seq: 1, Offsets: map[int]uint64{0: 5}}
	if err := store.Save(t.Context(), good); err != nil {
		t.Fatal(err)
	}
	bad := good
	bad.Version = 999
	if err := store.Save(t.Context(), bad); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Load(t.Context())
	if !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("不兼容检查点应返回 ErrIncompatibleCheckpoint, got %v", err)
	}
	t.Logf("不兼容检查点拒绝原因: %v", err)

	// 正常往返
	if err := store.Save(t.Context(), good); err != nil {
		t.Fatal(err)
	}
	cp, ok, err := store.Load(t.Context())
	if err != nil || !ok {
		t.Fatalf("加载失败: ok=%v err=%v", ok, err)
	}
	if cp.Offsets[0] != 5 || cp.Seq != 1 {
		t.Fatalf("检查点内容不一致: %+v", cp)
	}
}
