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
	// 接收边界按“事件时间 + 允许延迟”判定，与窗口 End 无关：
	// 接收条件为 eventTime+5 > 水位线（边界相等即拒绝）。
	//
	// 水位线 14 时：
	//   - @10：10+5=15 > 14，仍在允许延迟内，接收（哪怕它已经落后水位线 4s）；
	//   - @9 ：9+5=14 <= 14，必须拒绝——尽管其窗口 [0,10) 的关闭线是 15、尚未彻底关闭。
	if r := m.Add(Event{ID: "a", Key: "k", EventTime: ts(10), Value: 1}, ts(14)); r != RejectNone {
		t.Fatalf("事件时间+允许延迟仍大于水位线，应被接收（将产生修正）, got %s", r)
	}
	if r := m.Add(Event{ID: "b", Key: "k", EventTime: ts(9), Value: 1}, ts(14)); r != RejectLate {
		t.Fatalf("事件时间+允许延迟<=水位线，即使窗口未彻底关闭也必须拒绝, got %s", r)
	}
	// 边界相等即拒绝：水位线 15 时 @10（10+5=15）拒绝。
	if r := m.Add(Event{ID: "c", Key: "k", EventTime: ts(10), Value: 1}, ts(15)); r != RejectLate {
		t.Fatalf("边界相等时应按迟到拒绝, got %s", r)
	}
	// 状态不应被拒绝事件污染：只有 a（@10，属窗口 [10,20)）被计入。
	st := m.states[stateKey(Window{Start: ts(10), End: ts(20)}, "k")]
	if st == nil || st.Count != 1 || st.Sum != 1 {
		t.Fatalf("被拒事件不得修改聚合状态: %+v", st)
	}
	t.Logf("事件时间 late 边界: accept 当 eventTime+allowedLateness > watermark；@10/wm14 接收，@9/wm14、@10/wm15 拒绝 => %s", RejectLate)
}

func TestWindowFireAndCorrect(t *testing.T) {
	// 窗口 10s，允许延迟 10s：接收边界 = eventTime+10 > watermark；
	// 窗口 [0,10) 的回收（关闭）线 = End+10 = 20。
	m := NewWindowManager(10*time.Second, 10*time.Second)
	m.Add(Event{ID: "a", Key: "x", EventTime: ts(1), Value: 2}, ts(1))
	m.Add(Event{ID: "b", Key: "x", EventTime: ts(2), Value: 3}, ts(2))
	m.Add(Event{ID: "c", Key: "y", EventTime: ts(11), Value: 5}, ts(11))
	// 水位线 9：窗口 [0,10) 未到 End，不触发
	if outs := m.FireReady(ts(9)); len(outs) != 0 {
		t.Fatalf("不应有窗口触发, got %d", len(outs))
	}
	// 水位线 10：窗口 [0,10) 首次触发 Revision=1，状态保留（允许延迟内可修正）
	outs := m.FireReady(ts(10))
	if len(outs) != 1 || outs[0].Key != "x" || outs[0].Revision != 1 ||
		outs[0].Sum != 5 || outs[0].Count != 2 {
		t.Fatalf("首次触发结果错误: %+v", outs)
	}
	t.Logf("fire: window=[%s,%s) key=x revision=%d sum=%v count=%d",
		outs[0].Window.Start, outs[0].Window.End, outs[0].Revision, outs[0].Sum, outs[0].Count)
	if m.ActiveWindows() != 2 {
		t.Fatalf("首次触发后窗口状态应保留, active=%d want 2", m.ActiveWindows())
	}
	// 允许延迟内迟到数据并入：@3 在水位线 12 时，3+10=13>12，接收 -> Revision=2
	if r := m.Add(Event{ID: "late1", Key: "x", EventTime: ts(3), Value: 4}, ts(12)); r != RejectNone {
		t.Fatalf("允许延迟内迟到事件应被接收, got %s", r)
	}
	outs = m.FireReady(ts(12))
	if len(outs) != 1 || outs[0].Revision != 2 || outs[0].Sum != 9 || outs[0].Count != 3 {
		t.Fatalf("修正结果错误: %+v", outs)
	}
	// @5 在水位线 14 时，5+10=15>14，仍在允许延迟内 -> Revision=3
	if r := m.Add(Event{ID: "late2", Key: "x", EventTime: ts(5), Value: 1}, ts(14)); r != RejectNone {
		t.Fatalf("允许延迟内迟到事件应被接收, got %s", r)
	}
	// 同水位线 14 下 @3：3+10=13<=14，越过接收边界 -> 必须拒绝；
	// 此时窗口关闭线 20 远未到达、窗口状态仍在，但该事件不得并入、不得补发修正。
	if r := m.Add(Event{ID: "tooLateEarly", Key: "x", EventTime: ts(3), Value: 100}, ts(14)); r != RejectLate {
		t.Fatalf("超出允许延迟（窗口尚未彻底关闭）的事件也必须拒绝, got %s", r)
	}
	outs = m.FireReady(ts(14))
	if len(outs) != 1 || outs[0].Revision != 3 || outs[0].Sum != 10 || outs[0].Count != 4 {
		t.Fatalf("二次修正结果错误（被拒事件不得产生修正）: %+v", outs)
	}
	// 水位线 19：@9 的接收截止线 9+10=19<=19 拒绝；窗口关闭线 20 未到，状态仍保留。
	if r := m.Add(Event{ID: "tooLateAtEdge", Key: "x", EventTime: ts(9), Value: 100}, ts(19)); r != RejectLate {
		t.Fatalf("边界相等（eventTime+allowedLateness<=watermark）应拒绝, got %s", r)
	}
	if outs := m.FireReady(ts(19)); len(outs) != 0 {
		t.Fatalf("水位线 19 不应有新触发或修正: %+v", outs)
	}
	if m.ActiveWindows() != 2 {
		t.Fatalf("窗口关闭线 20 未到，状态不得提前回收, active=%d want 2", m.ActiveWindows())
	}
	// 水位线 20：[0,10) 到达回收边界被清理；同时 [10,20) 首次触发 y。
	outs = m.FireReady(ts(20))
	if len(outs) != 1 || outs[0].Key != "y" || outs[0].Revision != 1 || outs[0].Sum != 5 {
		t.Fatalf("应只触发下一窗口 y: %+v", outs)
	}
	if m.ActiveWindows() != 1 {
		t.Fatalf("窗口 [0,10) 状态应被清理, active=%d want 1", m.ActiveWindows())
	}
	// 窗口彻底关闭后再到达的事件被拒绝，结果不再改变
	if r := m.Add(Event{ID: "tooLate", Key: "x", EventTime: ts(0), Value: 100}, ts(20)); r != RejectLate {
		t.Fatalf("窗口关闭后事件应拒绝, got %s", r)
	}
	if outs := m.FireReady(ts(1000)); len(outs) != 0 {
		t.Fatalf("关闭后不应再产生任何输出: %+v", outs)
	}
}

func TestDedup(t *testing.T) {
	d := NewDedupStore()
	if !d.CheckAndMark("e1", ts(1)) {
		t.Fatal("首次应接收")
	}
	if d.CheckAndMark("e1", ts(1)) {
		t.Fatal("重复应拒绝")
	}
	// 窗口 [0,10) 允许延迟 5s => 关闭线 15：水位线 14 不回收，15 回收
	if n := d.CollectBefore(ts(14), 10*time.Second, 5*time.Second); n != 0 {
		t.Fatalf("窗口未关闭前不得回收去重 ID, removed=%d", n)
	}
	if n := d.CollectBefore(ts(15), 10*time.Second, 5*time.Second); n != 1 || d.Len() != 0 {
		t.Fatalf("窗口彻底关闭后应回收 ID, removed=%d remain=%d", n, d.Len())
	}
	// 回收后迟到的重复副本到达：其窗口已关闭会被 RejectLate 拒绝，结果不受影响
	snap := d.Snapshot()
	if len(snap) != 0 {
		t.Fatalf("回收后快照应为空, got %d", len(snap))
	}
	d2 := NewDedupStore()
	d2.CheckAndMark("e1", ts(1))
	d2.Restore(d2.Snapshot())
	if d2.CheckAndMark("e1", ts(1)) {
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

	// 不兼容的旧版本 v1：字段语义已变（无 Revision/Fired、去重无事件时间），必须拒绝
	good := Checkpoint{Version: FormatVersion, Seq: 1, Offsets: map[int]uint64{0: 5}}
	if err := store.Save(t.Context(), good); err != nil {
		t.Fatal(err)
	}
	old := good
	old.Version = 1
	if err := store.Save(t.Context(), old); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Load(t.Context())
	if !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("旧版本检查点应返回 ErrIncompatibleCheckpoint, got %v", err)
	}
	t.Logf("旧版本检查点拒绝原因: %v", err)

	// 不兼容的更新版本：明确拒绝，避免旧二进制误读新状态
	bad := good
	bad.Version = FormatVersion + 1
	if err := store.Save(t.Context(), bad); err != nil {
		t.Fatal(err)
	}
	_, _, err = store.Load(t.Context())
	if !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("更新版本检查点应返回 ErrIncompatibleCheckpoint, got %v", err)
	}
	t.Logf("更新版本检查点拒绝原因: %v", err)

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

// TestWindowRevisionSnapshotRestore：已触发并修正过的窗口状态，
// 经 Snapshot/Restore 后修订进度（Fired/Revision/聚合值）必须完整保留。
func TestWindowRevisionSnapshotRestore(t *testing.T) {
	m := NewWindowManager(10*time.Second, 10*time.Second)
	m.Add(Event{ID: "a", Key: "k", EventTime: ts(1), Value: 1}, ts(1))
	if outs := m.FireReady(ts(10)); len(outs) != 1 || outs[0].Revision != 1 {
		t.Fatalf("首次触发错误: %+v", outs)
	}
	if r := m.Add(Event{ID: "late", Key: "k", EventTime: ts(2), Value: 2}, ts(10)); r != RejectNone {
		t.Fatalf("允许延迟内应接收: %s", r)
	}
	if outs := m.FireReady(ts(10)); len(outs) != 1 || outs[0].Revision != 2 || outs[0].Sum != 3 {
		t.Fatalf("修正错误: %+v", outs)
	}

	// 快照 -> 新 manager 恢复：状态仍处于“已触发 Rev=2”，再来迟到应产出 Rev=3
	snap := m.Snapshot()
	m2 := NewWindowManager(10*time.Second, 10*time.Second)
	m2.Restore(snap)
	if r := m2.Add(Event{ID: "late2", Key: "k", EventTime: ts(3), Value: 3}, ts(10)); r != RejectNone {
		t.Fatalf("恢复后允许延迟内应继续接收: %s", r)
	}
	outs := m2.FireReady(ts(10))
	if len(outs) != 1 || outs[0].Revision != 3 || outs[0].Sum != 6 {
		t.Fatalf("恢复后修正进度应延续: %+v", outs)
	}
}

// TestCheckpointV1RejectedEndToEnd：落一个 v1 旧格式检查点，Recover 必须明确拒绝，
// 且拒绝后运行时不得把旧状态当成新状态继续。
func TestCheckpointV1RejectedEndToEnd(t *testing.T) {
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	v2 := Checkpoint{Version: FormatVersion, Seq: 1,
		Windows: []WindowState{{Key: "k", Revision: 1, Fired: true}}}
	if err := store.Save(t.Context(), v2); err != nil {
		t.Fatal(err)
	}
	v1 := v2
	v1.Version = 1
	if err := store.Save(t.Context(), v1); err != nil {
		t.Fatal(err)
	}
	cfg := Config{WindowSize: 10 * time.Second, CheckpointStore: store}
	rt := NewRuntime(cfg, &SliceSource{Data: [][]Event{{}}}, NewMemSink())
	ok, err := rt.Recover(t.Context())
	if !errors.Is(err, ErrIncompatibleCheckpoint) || ok {
		t.Fatalf("v1 检查点必须被明确拒绝: ok=%v err=%v", ok, err)
	}
	if wins, _ := rt.StateSizes(); wins != 0 {
		t.Fatalf("拒绝旧检查点后不得加载任何旧状态, wins=%d", wins)
	}
	t.Logf("v1 检查点拒绝原因: %v", err)
}
