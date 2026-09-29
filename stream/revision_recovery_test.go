package stream

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestRevisionRecovery 在"窗口已触发、部分修正已确认"的过程中崩溃，
// 从检查点恢复后验证：
//   - 已确认结果（含已确认修正版本）不再出现一遍；
//   - 未确认的修正版本被重放并收敛；
//   - 修正版本号不倒退、不重复；
//   - 恢复后到达的迟到数据仍能继续修正到正确终值。
func TestRevisionRecovery(t *testing.T) {
	const windowSize = 10 * time.Second
	const lateness = 10 * time.Second

	// 完整事件流（单分片，按时间有序）。
	build := func() []Event {
		evs := []Event{
			{ID: "a", Key: "k", EventTime: ts(1), Value: 1},
			{ID: "b", Key: "k", EventTime: ts(2), Value: 2},
			{ID: "h1", Key: "hb", EventTime: ts(10), Value: 0},   // -> 首次触发 rev=1
			{ID: "h2", Key: "hb", EventTime: ts(11), Value: 0},   // wm=11, cutoff=1
			{ID: "late1", Key: "k", EventTime: ts(3), Value: 10}, // -> 修正 rev=2
			{ID: "h3", Key: "hb", EventTime: ts(13), Value: 0},   // wm=13, cutoff=3
			{ID: "late2", Key: "k", EventTime: ts(4), Value: 20}, // -> 修正 rev=3
			{ID: "h4", Key: "hb", EventTime: ts(15), Value: 0},   // wm=15, cutoff=5
			{ID: "late3", Key: "k", EventTime: ts(6), Value: 30}, // -> 修正 rev=4（恢复后到达）
		}
		for i := range evs {
			evs[i].Shard = 0
			evs[i].Offset = uint64(i)
		}
		return evs
	}

	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	cfg := Config{
		WindowSize: windowSize, AllowedLateness: lateness,
		FetchBatch: 1, CheckpointEvery: 100, // 关闭运行中自动检查点，手动控制
		CheckpointStore: store,
	}
	cfg.Logger = func(format string, args ...any) { t.Logf(format, args...) }
	id := outID(0, "k")

	// ---- 第一次运行：处理到 late1（rev=2）后手动检查点，并让 rev=2 已确认 ----
	evs := build()
	src1 := newGatedSource(evs)
	sink := NewMemSink()
	rt := NewRuntime(cfg, src1, sink)
	done := make(chan error, 1)
	go func() { done <- rt.Run(context.Background()) }()
	for i := 0; i < 5; i++ { // a,b,h1,h2,late1
		if err := src1.deliver(context.Background(), rt); err != nil {
			t.Fatal(err)
		}
	}
	if err := rt.flushPending(context.Background()); err != nil { // 确认 rev=1 与 rev=2
		t.Fatal(err)
	}
	if rev := sink.Revision(id); rev != 2 {
		t.Fatalf("崩溃前下游应已收到 rev=2, got %d", rev)
	}
	if err := rt.checkpoint(context.Background()); err != nil { // 记录 acks rev=2
		t.Fatal(err)
	}
	writesBeforeCrash := sink.Writes

	// ---- 模拟崩溃：中止第一次运行的 goroutine，保留 sink 与检查点 ----
	src1.abortErr = errCrash
	close(src1.abort)
	if err := <-done; err != errCrash {
		t.Fatalf("崩溃运行应返回 errCrash, got %v", err)
	}

	// ---- 第二次运行：从检查点恢复，重放完整流 ----
	src2 := newGatedSource(evs)
	rt2 := NewRuntime(cfg, src2, sink)
	ok, err := rt2.Recover(context.Background())
	if err != nil || !ok {
		t.Fatalf("恢复失败: ok=%v err=%v", ok, err)
	}
	// 恢复出的未确认队列不得包含已确认（rev<=2）的输出。
	rt2.mu.Lock()
	for _, out := range rt2.pending {
		if out.ID == id && out.Revision <= rt2.acks[id] {
			t.Fatalf("已确认版本被恢复进 pending: %+v acks=%d", out, rt2.acks[id])
		}
	}
	resumeOffset := rt2.offsets[0]
	ackRev := rt2.acks[id]
	rt2.mu.Unlock()
	if resumeOffset != 5 {
		t.Fatalf("应从位点 5 继续, got %d", resumeOffset)
	}
	if ackRev != 2 {
		t.Fatalf("修正进度应恢复为已确认 rev=2, got %d", ackRev)
	}
	// Run 开头会先冲刷恢复出的 pending：已确认版本必须被过滤，不产生下游写入。
	if err := rt2.flushPending(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sink.Writes != writesBeforeCrash {
		t.Fatalf("已确认的 rev<=2 恢复后不得重发给下游: crash 前写入=%d, 现在=%d",
			writesBeforeCrash, sink.Writes)
	}
	if rev := sink.Revision(id); rev != 2 {
		t.Fatalf("恢复后下游版本不应倒退: got %d, want 2", rev)
	}

	// 恢复后在后台跑 Run，前台放行剩余 4 条事件（位点 5 起：h3,late2,h4,late3）。
	done2 := make(chan error, 1)
	go func() { done2 <- rt2.Run(context.Background()) }()
	for i := 0; i < 4; i++ {
		if err := src2.deliver(context.Background(), rt2); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done2; err != nil {
		t.Fatal(err)
	}

	// 最终：rev=4，sum=1+2+10+20+30=63, count=5
	final := sink.seen[id]
	if final.Revision != 4 || final.Sum != 63 || final.Count != 5 {
		t.Fatalf("恢复后修正终值错误: rev=%d sum=%v count=%d, want rev=4 sum=63 count=5",
			final.Revision, final.Sum, final.Count)
	}
	// 恢复后去重仍生效：前 5 条事件从位点 5 之后不再处理，无重复计数。
	st := rt2.Stats()
	if st.Duplicates != 0 {
		t.Fatalf("恢复后重放不应产生重复事件, got %d", st.Duplicates)
	}
	// 与参考聚合一致。
	refEvents := []Event{
		{ID: "a", Key: "k", EventTime: ts(1), Value: 1},
		{ID: "b", Key: "k", EventTime: ts(2), Value: 2},
		{ID: "late1", Key: "k", EventTime: ts(3), Value: 10},
		{ID: "late2", Key: "k", EventTime: ts(4), Value: 20},
		{ID: "late3", Key: "k", EventTime: ts(6), Value: 30},
		{ID: "h1", Key: "hb", EventTime: ts(10), Value: 0},
		{ID: "h2", Key: "hb", EventTime: ts(11), Value: 0},
		{ID: "h3", Key: "hb", EventTime: ts(13), Value: 0},
		{ID: "h4", Key: "hb", EventTime: ts(15), Value: 0},
	}
	assertConsistent(t, resultsMap(sink), reference([][]Event{refEvents}, windowSize))
	t.Logf("恢复修正语义验证通过: 最终 rev=%d sum=%v count=%d, 崩溃后新增写入=%d",
		final.Revision, final.Sum, final.Count, sink.Writes-writesBeforeCrash)
}

// errSink 模拟下游瞬时写入失败。
var errSink = errors.New("模拟下游写入失败")

// failRevSink 对指定 revision 的第一次写入返回错误（模拟下游失败/进程中断），
// 其余写入委托给内层 MemSink。
type failRevSink struct {
	inner   *MemSink
	failRev int
	failed  bool
}

func (f *failRevSink) Write(ctx context.Context, out Output) error {
	if !f.failed && out.Revision == f.failRev {
		f.failed = true
		return errSink
	}
	return f.inner.Write(ctx, out)
}

// TestRevisionRecoveryUnconfirmed 崩溃时修正版本已产出但未确认（在 pending 中）：
// 恢复后该版本必须重发且恰好一次，随后继续修正到终值。
func TestRevisionRecoveryUnconfirmed(t *testing.T) {
	const windowSize = 10 * time.Second
	const lateness = 10 * time.Second
	evs := []Event{
		{ID: "a", Key: "k", EventTime: ts(1), Value: 1},
		{ID: "b", Key: "k", EventTime: ts(2), Value: 2},
		{ID: "h1", Key: "hb", EventTime: ts(10), Value: 0},
		{ID: "h2", Key: "hb", EventTime: ts(11), Value: 0},
		{ID: "late1", Key: "k", EventTime: ts(3), Value: 10}, // rev=2，写入失败 -> 未确认
		{ID: "h3", Key: "hb", EventTime: ts(13), Value: 0},
		{ID: "late2", Key: "k", EventTime: ts(4), Value: 20}, // rev=3，恢复后到达
	}
	for i := range evs {
		evs[i].Shard = 0
		evs[i].Offset = uint64(i)
	}
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	cfg := Config{
		WindowSize: windowSize, AllowedLateness: lateness,
		FetchBatch: 1, CheckpointEvery: 100, CheckpointStore: store,
	}
	cfg.Logger = func(format string, args ...any) { t.Logf(format, args...) }
	id := outID(0, "k")

	// 第一次运行：rev=1 确认；rev=2 写入失败，Run 落检查点后返回，rev=2 留在 pending。
	memSink := NewMemSink()
	sink := &failRevSink{inner: memSink, failRev: 2}
	rt := NewRuntime(cfg, &SliceSource{Data: [][]Event{evs}}, sink)
	if err := rt.Run(context.Background()); !errors.Is(err, errSink) {
		t.Fatalf("应返回下游错误, got %v", err)
	}
	if memSink.Revision(id) != 1 {
		t.Fatalf("崩溃时下游应只到 rev=1, got %d", memSink.Revision(id))
	}
	writesAtCrash := memSink.Writes

	// 第二次运行：恢复后 Run 开头先重发 pending 中的 rev=2（failRevSink 已不再失败）。
	src2 := newGatedSource(evs)
	rt2 := NewRuntime(cfg, src2, sink)
	ok, err := rt2.Recover(context.Background())
	if err != nil || !ok {
		t.Fatalf("恢复失败: ok=%v err=%v", ok, err)
	}
	done2 := make(chan error, 1)
	go func() { done2 <- rt2.Run(context.Background()) }()
	// Run 开头重放 rev=2；随后放行剩余 2 条（h3, late2）-> rev=3。
	for i := 0; i < 2; i++ {
		if err := src2.deliver(context.Background(), rt2); err != nil {
			t.Fatal(err)
		}
	}
	if err := <-done2; err != nil {
		t.Fatal(err)
	}
	final := memSink.seen[id]
	if final.Revision != 3 || final.Sum != 33 || final.Count != 4 {
		t.Fatalf("终值错误: rev=%d sum=%v count=%d", final.Revision, final.Sum, final.Count)
	}
	// 崩溃后写入恰好为：rev=2 重放 1 次 + rev=3 修正 1 次 + 结束时 hb 窗口首次触发 1 次。
	if memSink.Writes != writesAtCrash+3 {
		t.Fatalf("写入次数不符: 崩溃时=%d 最终=%d", writesAtCrash, memSink.Writes)
	}
	// k 的每个版本在下游恰好一份，且终值正确。
	if memSink.seen[id].Revision != 3 {
		t.Fatalf("k 终值版本错误: %d", memSink.seen[id].Revision)
	}
	t.Logf("未确认修正重放验证通过: rev1 不重发, rev2 恰好重发一次, 终值 rev=3, 总写入=%d", memSink.Writes)
}

// TestCheckpointVersionReject 验证旧版本（v1）与更新版本（v3）检查点均被明确拒绝。
func TestCheckpointVersionReject(t *testing.T) {
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	for _, v := range []int{1, 3} {
		cp := Checkpoint{Version: v, Seq: 1, Offsets: map[int]uint64{0: 1}}
		if err := store.Save(t.Context(), cp); err != nil {
			t.Fatal(err)
		}
		_, _, err := store.Load(t.Context())
		if !errors.Is(err, ErrIncompatibleCheckpoint) {
			t.Fatalf("版本 %d 必须被拒绝, got %v", v, err)
		}
		t.Logf("版本 %d 检查点拒绝原因: %v", v, err)
	}
	// 当前版本可正常加载。
	if err := store.Save(t.Context(), Checkpoint{
		Version: FormatVersion, Seq: 2, Offsets: map[int]uint64{0: 9},
	}); err != nil {
		t.Fatal(err)
	}
	cp, ok, err := store.Load(t.Context())
	if err != nil || !ok || cp.Offsets[0] != 9 {
		t.Fatalf("当前版本往返失败: ok=%v err=%v cp=%+v", ok, err, cp)
	}
}
