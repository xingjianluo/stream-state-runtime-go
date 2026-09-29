package stream

import (
	"context"
	"errors"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"
)

// 构造“先触发、后多次迟到修正”的单分片输入（处理顺序严格等于输入顺序，
// 不依赖归并调度，便于精确刻画水位线、首次触发与每次修正）。
// 窗口 [0,10) 键 k：触发线 10，关闭线 10+允许延迟(10s)=20。
//
//	v=1@1, v=2@2          ：聚合，水位线低，不触发
//	water@10              ：首次触发 Revision=1（sum=3）
//	v=3@3, v=4@4, v=5@5   ：允许延迟内迟到 -> Revision=2,3,4（sum=6,10,15）
//	water@20              ：推过关闭线，窗口彻底关闭清理
func lateCorrectionShards() [][]Event {
	ev := func(id string, off uint64, sec int, v float64) Event {
		return Event{ID: id, Key: "k", Shard: 0, Offset: off, EventTime: ts(sec), Value: v}
	}
	return [][]Event{{
		ev("a", 0, 1, 1),
		ev("b", 1, 2, 2),
		ev("t", 2, 10, 0),
		ev("c", 3, 3, 3),
		ev("d", 4, 4, 4),
		ev("e", 5, 5, 5),
		ev("z", 6, 20, 0),
	}}
}

func correctionCfg() Config {
	return Config{
		WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second,
		FetchBatch: 1, InFlightLimit: 4, CheckpointEvery: 100,
	}
}

// TestLateCorrectionConverges：同一窗口被多次迟到数据修正后逐步收敛到
// “所有数据一次性算完”的结果，且每一步 Revision 单调、数值逐步逼近终值。
func TestLateCorrectionConverges(t *testing.T) {
	shards := lateCorrectionShards()
	cfg := correctionCfg()
	cfg.Logger = func(f string, a ...any) { t.Logf(f, a...) }
	sink := NewMemSink()
	rt := NewRuntime(cfg, &SliceSource{Data: shards}, sink)
	if err := rt.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	const id = "0-10000000000/k"
	revs := sink.Revisions(id)
	wantRevs := []int64{1, 2, 3, 4}
	if len(revs) != len(wantRevs) {
		t.Fatalf("生效修订序列长度=%d, want %d (%v)", len(revs), len(wantRevs), revs)
	}
	for i := range wantRevs {
		if revs[i] != wantRevs[i] {
			t.Fatalf("生效修订序列=%v, want %v", revs, wantRevs)
		}
	}
	// 逐步收敛：每次修正都应更新为更大的累计值
	wantSums := []float64{3, 6, 10, 15}
	for i, want := range wantSums {
		if got := sink.Applied(id)[i].Sum; got != want {
			t.Fatalf("Revision=%d 时 sum=%v, want %v", i+1, got, want)
		}
	}
	got, ok := sink.Get(id)
	if !ok || got.Sum != 15 || got.Count != 5 {
		t.Fatalf("最终结果 %+v ok=%v, want sum=15 count=5", got, ok)
	}
	// 与“所有数据一次性算完”的参考聚合一致
	assertConsistent(t, resultsMap(sink), reference(shards, cfg.WindowSize))
	if rt.Stats().Corrections != 3 {
		t.Fatalf("Corrections=%d, want 3", rt.Stats().Corrections)
	}
	t.Logf("修正收敛: revisions=%v finalSum=%v count=%d", revs, got.Sum, got.Count)
}

// TestCorrectionRetryIdempotent：模拟同一修订版被重复投递（进程内重试或恢复后重放），
// 以及旧修订版在新版本之后才到达：下游必须“同版去重、旧版不回退”，每个修订版恰好生效一次。
func TestCorrectionRetryIdempotent(t *testing.T) {
	ctx := t.Context()
	sink := NewMemSink()
	base := Output{ID: "w/k", Key: "k", Window: Window{Start: ts(0), End: ts(10)}}
	put := func(rev int64, sum float64) {
		o := base
		o.Revision, o.Sum = rev, sum
		if err := sink.Write(ctx, o); err != nil {
			t.Fatalf("Write revision=%d 失败: %v", rev, err)
		}
	}

	// Revision=1 因重试被投递 3 次：只生效一次
	put(1, 3)
	put(1, 3)
	put(1, 3)
	// Revision=2 投递两次（一次正常、一次重试）：只生效一次
	put(2, 6)
	put(2, 6)
	// 旧版 Revision=1 在新版之后才到达（乱序/重放）：必须被忽略，不回退
	put(1, 3)
	// Revision=3 一次
	put(3, 10)
	// 再次乱序重放旧版 2、1：均被忽略
	put(2, 6)
	put(1, 3)

	revs := sink.Revisions(base.ID)
	want := []int64{1, 2, 3}
	if len(revs) != len(want) {
		t.Fatalf("生效序列=%v, want %v（同版重复与旧版回退都必须被忽略）", revs, want)
	}
	for i := range want {
		if revs[i] != want[i] {
			t.Fatalf("生效序列=%v, want %v", revs, want)
		}
	}
	if got := sink.Results(); len(got) != 1 || got[0].Revision != 3 || got[0].Sum != 10 {
		t.Fatalf("最终应只有最新版 Revision=3 sum=10, got %+v", got)
	}
	t.Logf("同版去重 + 旧版不回退: 生效序列=%v, Write 调用=%d", revs, sink.Writes)
}

// TestCorrectionOrderDeterministic：多分片交错下多次运行，
// 每个输出的修订序列必须完全一致（版本与先后确定）。
func TestCorrectionOrderDeterministic(t *testing.T) {
	shards := lateCorrectionShards()
	cfg := correctionCfg()
	var first []int64
	for run := 0; run < 5; run++ {
		sink := NewMemSink()
		rt := NewRuntime(cfg, &SliceSource{Data: shards}, sink)
		if err := rt.Run(t.Context()); err != nil {
			t.Fatal(err)
		}
		revs := sink.Revisions("0-10000000000/k")
		if run == 0 {
			first = revs
			continue
		}
		if len(revs) != len(first) {
			t.Fatalf("第 %d 次运行修订序列长度不一致: %v vs %v", run, revs, first)
		}
		for i := range first {
			if revs[i] != first[i] {
				t.Fatalf("第 %d 次运行修订序列不一致: %v vs %v", run, revs, first)
			}
		}
	}
	t.Logf("5 次运行修订序列完全一致: %v", first)
}

func keyName(k int) string { return "k" + strconv.Itoa(k) }

// makeSmoothStream 生成“事件时间逐条平滑推进”的两分片流：
// 第 i 条事件的事件时间为 i 秒，按 i%2 落到分片，键在 keys 个之间循环。
// 因此 round-robin 处理顺序与事件时间顺序一致，任意时刻只有常数个活跃窗口；
// 用于断言长期运行下窗口状态与去重集合的峰值与总事件量无关。
// makeSmoothStream 生成两分片“时间齐步推进”的流：每一对全局步（step）在
// 两个分片各放一条、事件时间相同（step 秒），键不同。round-robin 归并下无论调度
// 如何，每消费一对事件水位线至多前进 1 秒，因此任意时刻只有常数个活跃窗口。
func makeSmoothStream(steps, keysPerWindow int) [][]Event {
	sh := [][]Event{{}, {}}
	off := []uint64{0, 0}
	for step := 0; step < steps; step++ {
		sec := step
		for s := 0; s < 2; s++ {
			k := (step*2 + s) % keysPerWindow
			sh[s] = append(sh[s], Event{
				ID:  "t-" + strconv.Itoa(step) + "-" + strconv.Itoa(s),
				Key: keyName(k), Shard: s, Offset: off[s],
				EventTime: ts(sec), Value: 1,
			})
			off[s]++
		}
	}
	return sh
}

// samplingSink 在每次下游写入时记录运行时内存状态规模的峰值。
type samplingSink struct {
	inner      Sink
	rt         *Runtime
	mu         sync.Mutex
	maxWindows int
	maxDedup   int
}

func (s *samplingSink) Write(ctx context.Context, out Output) error {
	if err := s.inner.Write(ctx, out); err != nil {
		return err
	}
	w, d := s.rt.StateSizes()
	s.mu.Lock()
	if w > s.maxWindows {
		s.maxWindows = w
	}
	if d > s.maxDedup {
		s.maxDedup = d
	}
	s.mu.Unlock()
	return nil
}

// TestStateSizeBounded：长时间大流量结束后，窗口状态与去重集合必须全部回收，
// 且处理量与回收量一致（没有任何状态随时间残留）。
func TestStateSizeBounded(t *testing.T) {
	const steps = 800
	total := steps * 2
	sh := makeSmoothStream(steps, 4)
	cfg := Config{
		WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second,
		FetchBatch: 4, InFlightLimit: 32, CheckpointEvery: 100000,
	}
	sink := NewMemSink()
	rt := NewRuntime(cfg, &SliceSource{Data: sh}, sink)
	if err := rt.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	wins, dedup := rt.StateSizes()
	st := rt.Stats()
	t.Logf("总事件=%d 处理=%d 去重GC=%d 末态: 窗口=%d 去重=%d",
		total, st.Processed, st.DedupGC, wins, dedup)
	if wins != 0 || dedup != 0 {
		t.Fatalf("结束后状态应全部回收, got 窗口=%d 去重=%d", wins, dedup)
	}
	if st.Processed != total {
		t.Fatalf("Processed=%d, want %d", st.Processed, total)
	}
	if st.DedupGC != total {
		t.Fatalf("DedupGC=%d, want %d（长期运行必须持续回收）", st.DedupGC, total)
	}
}

// TestStateSizePeakBounded：运行中途窗口状态数与去重集合峰值必须是与
// “活跃窗口跨度×键数”相关的常数，不随总时长（窗口总数）增长。
// 通过把流拉长到两组不同规模，验证峰值基本不变。
func TestStateSizePeakBounded(t *testing.T) {
	cfg := Config{
		WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second,
		FetchBatch: 1, InFlightLimit: 2, CheckpointEvery: 1000000,
	}
	// 单分片事件时间严格按位点秒递增：组合水位线=事件时间，平滑前进，
	// 直接度量“状态驻留峰值只与窗口/允许延迟跨度有关、与总时长无关”。
	measure := func(total int) (int, int) {
		events := make([]Event, total)
		for i := 0; i < total; i++ {
			events[i] = Event{
				ID: "m-" + strconv.Itoa(i), Key: keyName(i % 4), Shard: 0,
				Offset: uint64(i), EventTime: ts(i), Value: 1,
			}
		}
		rt := NewRuntime(cfg, &SliceSource{Data: [][]Event{events}}, nil)
		sampler := &samplingSink{inner: NewMemSink(), rt: rt}
		rt.sink = sampler
		if err := rt.Run(t.Context()); err != nil {
			t.Fatal(err)
		}
		return sampler.maxWindows, sampler.maxDedup
	}
	w1, d1 := measure(400)
	w2, d2 := measure(3200) // 8 倍步数
	t.Logf("峰值(400 步): 窗口=%d 去重=%d；峰值(3200 步): 窗口=%d 去重=%d", w1, d1, w2, d2)
	// 允许延迟=一个窗口长度：任意时刻活跃窗口为常数个（未触发+已触发待关闭），
	// 去重保留一个关闭窗口宽度的 ID。8 倍流量峰值不得随之显著放大（给小余量）。
	const winBound = 24
	const dedupBound = 64
	if w1 > winBound || w2 > winBound {
		t.Fatalf("窗口状态峰值超过常数上界 %d: 短流=%d 长流=%d", winBound, w1, w2)
	}
	if d1 > dedupBound || d2 > dedupBound {
		t.Fatalf("去重集合峰值超过常数上界 %d: 短流=%d 长流=%d", dedupBound, d1, d2)
	}
}

// TestCorrectionRecovery：崩溃 → 检查点恢复后，修正语义必须仍然正确：
//   - 已确认（已随检查点冲刷）的修订版不再出现；
//   - 未确认的修订版恢复后重放，Sink 幂等不产生重复；
//   - 修订进度不倒退；恢复之后再到达的迟到数据能在正确的 Revision 上继续修正；
//   - 最终结果与“一次性算完”一致。
func TestCorrectionRecovery(t *testing.T) {
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	// 单分片平滑流：事件时间=下标秒，便于在窗口 [0,10) 上制造首次触发与迟到修正。
	// 前 11 条推进到首次触发（Revision=1），随后允许延迟内迟到若干条，
	// 用 crashSource 在中途崩溃，再从检查点恢复并继续迟到数据。
	mkEvents := func() []Event {
		es := make([]Event, 0, 30)
		add := func(id string, sec int, v float64) {
			es = append(es, Event{ID: id, Key: "k", Shard: 0,
				Offset: uint64(len(es)), EventTime: ts(sec), Value: v})
		}
		add("a", 1, 1)
		add("b", 2, 2)
		add("t", 10, 0) // 首次触发 [0,10) Revision=1（sum=3）
		add("c", 3, 3)  // Revision=2
		add("d", 4, 4)  // Revision=3  —— 在这附近崩溃
		add("e", 5, 5)  // Revision=4（恢复后继续）
		add("f", 6, 6)  // Revision=5（恢复后继续）
		add("z", 30, 0) // 推过关闭线（10+10=20）
		return es
	}
	allEvents := mkEvents()

	cfg := Config{
		WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second,
		FetchBatch: 1, InFlightLimit: 4, CheckpointEvery: 3,
		CheckpointStore: store,
	}
	cfg.Logger = func(f string, a ...any) { t.Logf(f, a...) }
	sink := NewMemSink()

	// 第一次运行：拉取若干批后崩溃（crashSource 对同一分片多次 Fetch 计数）。
	crash := &crashSource{inner: &SliceSource{Data: [][]Event{allEvents}}, limit: 4}
	rt1 := NewRuntime(cfg, crash, sink)
	err := rt1.Run(t.Context())
	if !errors.Is(err, errCrash) {
		t.Fatalf("应返回模拟崩溃错误, got %v", err)
	}
	beforeRevs := sink.Revisions("0-10000000000/k")
	beforeWrites := sink.Writes
	t.Logf("崩溃前已生效修订=%v sink.Write=%d", beforeRevs, beforeWrites)

	// 恢复并跑完剩余数据
	rt2 := NewRuntime(cfg, &SliceSource{Data: [][]Event{allEvents}}, sink)
	ok, err := rt2.Recover(t.Context())
	if err != nil || !ok {
		t.Fatalf("恢复失败: ok=%v err=%v", ok, err)
	}
	if err := rt2.Run(t.Context()); err != nil {
		t.Fatal(err)
	}

	revs := sink.Revisions("0-10000000000/k")
	t.Logf("恢复后总生效修订=%v sink.Write=%d", revs, sink.Writes)
	// 最终应包含 Revision=1..5，每个恰好一次（无重复、无倒退）
	want := []int64{1, 2, 3, 4, 5}
	if len(revs) != len(want) {
		t.Fatalf("恢复后生效修订=%v, want %v（每个修订版恰好一次）", revs, want)
	}
	for i := range want {
		if revs[i] != want[i] {
			t.Fatalf("恢复后生效修订=%v, want %v（修订进度不得倒退/重复）", revs, want)
		}
	}
	got, exists := sink.Get("0-10000000000/k")
	if !exists || got.Sum != 21 || got.Count != 6 {
		t.Fatalf("最终结果 %+v exists=%v, want sum=21 count=6", got, exists)
	}
	// 与“所有数据一次性算完”的参考聚合一致
	assertConsistent(t, resultsMap(sink), reference([][]Event{allEvents}, cfg.WindowSize))
	t.Logf("恢复后修正语义正确: 修订=%v finalSum=%v count=%d", revs, got.Sum, got.Count)
}
