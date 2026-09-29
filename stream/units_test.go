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
	if r, _ := m.Add(Event{ID: "a", Key: "k", EventTime: ts(8), Value: 1}, wm); r != RejectNone {
		t.Fatalf("事件应被接收, got %s", r)
	}
	if r, _ := m.Add(Event{ID: "b", Key: "k", EventTime: ts(7), Value: 1}, wm); r != RejectLate {
		t.Fatalf("迟到事件应被拒绝, got %s", r)
	}
	t.Logf("late 拒绝判定: eventTime=%s watermark=%s allowedLateness=5s => %s", ts(7), wm, RejectLate)
}

func TestWindowFireReady(t *testing.T) {
	m := NewWindowManager(10*time.Second, 5*time.Second)
	m.Add(Event{ID: "a", Key: "x", EventTime: ts(1), Value: 2}, ts(1))
	m.Add(Event{ID: "b", Key: "x", EventTime: ts(2), Value: 3}, ts(2))
	m.Add(Event{ID: "c", Key: "y", EventTime: ts(11), Value: 5}, ts(2))
	// 水位线 9：窗口[0,10) 的 End=10 未到，不触发
	if outs := m.EmitReady(ts(9)); len(outs) != 0 {
		t.Fatalf("不应有窗口触发, got %d", len(outs))
	}
	// 水位线 10：窗口[0,10) 首次触发，版本 1；状态保留以接收迟到数据
	outs := m.EmitReady(ts(10))
	if len(outs) != 1 || outs[0].Key != "x" || outs[0].Sum != 5 || outs[0].Count != 2 || outs[0].Revision != 1 {
		t.Fatalf("触发结果错误: %+v", outs)
	}
	id := outs[0].ID
	// 重复触发不得再次产出
	if outs := m.EmitReady(ts(10)); len(outs) != 0 {
		t.Fatalf("已触发窗口不得重复首次触发: %+v", outs)
	}
	// 容忍期内的迟到数据：wm=11, cutoff=6, eventTime=7 被接收并产生版本 2 的修正
	r, corrected := m.Add(Event{ID: "d", Key: "x", EventTime: ts(7), Value: 10}, ts(11))
	if r != RejectNone || !corrected {
		t.Fatalf("容忍期内迟到事件应被接收并标记为修正: reason=%s corrected=%v", r, corrected)
	}
	c1, ok := m.EmitCorrection(m.Assign(ts(7)), "x", ts(11))
	if !ok || c1.ID != id || c1.Revision != 2 || c1.Sum != 15 || c1.Count != 3 {
		t.Fatalf("修正结果错误: %+v ok=%v", c1, ok)
	}
	// 超出容忍边界（eventTime <= wm-L）的事件仍被拒绝
	if r, _ := m.Add(Event{ID: "e", Key: "x", EventTime: ts(5), Value: 1}, ts(11)); r != RejectLate {
		t.Fatalf("超出容忍期的迟到事件应被拒绝, got %s", r)
	}
	// wm=14 尚未到清理线（End+Lateness=15），状态必须保留，还能再修正一次
	if purged := m.PurgeExpired(ts(14)); len(purged) != 0 {
		t.Fatalf("容忍期未结束不得清理状态, got %d", len(purged))
	}
	if !m.HasOutputID(id) {
		t.Fatal("容忍期内窗口状态应仍保留")
	}
	if _, corrected := m.Add(Event{ID: "f", Key: "x", EventTime: ts(9), Value: 20}, ts(13)); !corrected {
		t.Fatal("eventTime=9 > cutoff=8 应被接收为修正")
	}
	c2, ok := m.EmitCorrection(m.Assign(ts(9)), "x", ts(13))
	if !ok || c2.Revision != 3 || c2.Sum != 35 || c2.Count != 4 {
		t.Fatalf("第二次修正错误: %+v", c2)
	}
	// wm=15 到达清理线：窗口状态被回收，此后不可能再有修正
	purged := m.PurgeExpired(ts(15))
	if len(purged) != 1 || purged[0].Key != "x" {
		t.Fatalf("到期清理结果错误: %+v", purged)
	}
	if m.HasOutputID(id) {
		t.Fatal("到期后窗口状态应已清理")
	}
	if r, _ := m.Add(Event{ID: "g", Key: "x", EventTime: ts(9), Value: 1}, ts(15)); r != RejectLate {
		t.Fatalf("清理后迟到事件必须被拒绝, got %s", r)
	}
	// 剩余窗口 [10,20) 的 y 在最终水位线触发
	if outs := m.EmitReady(ts(1000)); len(outs) != 1 || outs[0].Key != "y" || outs[0].Revision != 1 {
		t.Fatalf("剩余窗口触发错误: %+v", outs)
	}
	t.Logf("窗口生命周期验证通过: 首次 rev=1, 两次修正 rev=2/3, 到期清理, 最终值 sum=%v count=%d",
		c2.Sum, c2.Count)
}

func TestDedup(t *testing.T) {
	d := NewDedupStore()
	if !d.CheckAndMark("e1", ts(15)) { // 所属窗口清理时刻
		t.Fatal("首次应接收")
	}
	if d.CheckAndMark("e1", ts(15)) {
		t.Fatal("重复应拒绝")
	}
	snap := d.Snapshot()
	if len(snap) != 1 || snap[0].ID != "e1" || snap[0].PurgeAt != ts(15).UnixNano() {
		t.Fatalf("快照内容错误: %+v", snap)
	}
	d2 := NewDedupStore()
	d2.Restore(snap)
	if d2.CheckAndMark("e1", ts(15)) {
		t.Fatal("恢复后重复应拒绝")
	}
}

func TestDedupGCBoundary(t *testing.T) {
	d := NewDedupStore()
	d.CheckAndMark("old", ts(15))  // 窗口 [0,10)+5s 容忍：清理时刻 15
	d.CheckAndMark("edge", ts(15)) // 恰好等于水位线：删除
	d.CheckAndMark("new", ts(25))  // 下一窗口，清理时刻 25：保留
	// 水位线 15：只有所属窗口已过清理线的 ID 被回收
	if n := d.GCExpired(ts(14)); n != 0 || d.Len() != 3 {
		t.Fatalf("水位线未到清理线时不得回收: removed=%d retained=%d", n, d.Len())
	}
	if n := d.GCExpired(ts(15)); n != 2 || d.Len() != 1 {
		t.Fatalf("GC 边界错误: removed=%d retained=%d", n, d.Len())
	}
	if d.CheckAndMark("old", ts(15)) != true {
		t.Fatal("已回收 ID 可再次标记（其事件此刻只会被判为迟到）")
	}
	// 容忍期窗口的 ID 仍能识别重复投递
	if d.CheckAndMark("new", ts(25)) {
		t.Fatal("未过清理线的 ID 仍须识别重复投递")
	}
	t.Logf("dedup GC 安全边界验证通过: 仅回收所属窗口 End+Lateness <= 水位线 的 ID")
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
