package stream

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// TestLateEventBeyondAllowedLatenessRejected：稳定复现“允许延迟被窗口大小放大”的边界。
//
// 场景（窗口 10s、允许延迟 5s）：
//   - 先用一条时间较新的事件把组合水位线推到 17；
//   - 随后到达一条事件时间 @11 的旧事件：它落后水位线 6s > 允许延迟 5s
//     （11+5=16 <= 17，越过接收边界），按配置语义必须直接按迟到拒绝；
//   - 但其所属窗口 [10,20) 的彻底关闭线是 20+5=25，此刻窗口尚未彻底关闭。
//     旧实现按“窗口 End+允许延迟”判定，会把这条事件错误收进去并补发一份修正。
//
// 断言：该事件计入 LateDropped，不计入 Processed，不改变任何聚合，
// 下游不出现属于它的额外结果或修正；窗口 [10,20) 只有新事件自身的一条 Revision=1。
func TestLateEventBeyondAllowedLatenessRejected(t *testing.T) {
	events := []Event{
		{ID: "a", Key: "k", Shard: 0, Offset: 0, EventTime: ts(1), Value: 1},
		{ID: "new", Key: "k", Shard: 0, Offset: 1, EventTime: ts(17), Value: 2}, // 水位线 -> 17
		{ID: "stale", Key: "k", Shard: 0, Offset: 2, EventTime: ts(11), Value: 99},
	}
	cfg := Config{
		WindowSize: 10 * time.Second, AllowedLateness: 5 * time.Second,
		FetchBatch: 1, InFlightLimit: 2, CheckpointEvery: 100,
	}
	cfg.Logger = func(f string, a ...any) { t.Logf(f, a...) }
	sink := NewMemSink()
	rt := newTestRuntime(t, [][]Event{events}, cfg, sink)
	if err := rt.Run(t.Context()); err != nil {
		t.Fatal(err)
	}

	st := rt.Stats()
	if st.LateDropped != 1 {
		t.Fatalf("LateDropped=%d, want 1（越过允许延迟的旧事件必须拒绝）", st.LateDropped)
	}
	if st.Processed != 2 {
		t.Fatalf("Processed=%d, want 2（被拒事件不得计入处理量）", st.Processed)
	}
	if st.Corrections != 0 {
		t.Fatalf("Corrections=%d, want 0（窗口尚未首次触发，且被拒事件不得补发任何结果）", st.Corrections)
	}
	if st.Duplicates != 0 {
		t.Fatalf("Duplicates=%d, want 0", st.Duplicates)
	}

	// 窗口 [10,20)：只含 new（value=2），stale 不得影响其聚合。
	got, ok := sink.Get("10000000000-20000000000/k")
	if !ok {
		t.Fatal("窗口 [10,20) 应有首次触发结果")
	}
	if got.Revision != 1 || got.Sum != 2 || got.Count != 1 {
		t.Fatalf("窗口 [10,20) 结果 %+v, want Revision=1 sum=2 count=1（被拒旧事件不得并入/修正）", got)
	}
	if revs := sink.Revisions("10000000000-20000000000/k"); len(revs) != 1 || revs[0] != 1 {
		t.Fatalf("窗口 [10,20) 下游修订序列=%v, want [1]（不得补发修正）", revs)
	}
	// 窗口 [0,10)：只含 a，结果同样不受影响。
	if g, ok := sink.Get("0-10000000000/k"); !ok || g.Sum != 1 || g.Count != 1 {
		t.Fatalf("窗口 [0,10) 结果 %+v ok=%v, want sum=1 count=1", g, ok)
	}
	// 与“只含被接收事件”的参考聚合一致（等价于把 stale 从全量输入中剔除）。
	accepted := [][]Event{{events[0], events[1]}}
	assertConsistent(t, resultsMap(sink), reference(accepted, cfg.WindowSize))
}

// TestAllowedLatenessNotScaledByWindow：接收边界只取决于“事件时间 + 允许延迟”与水位线，
// 与窗口大小、事件落在窗口什么位置无关，允许延迟绝不被窗口大小放大。
//
// 每个用例先放入一条 base 事件建立窗口状态，再用 end 事件把水位线推到窗口 End 触发
// Revision=1（sum=2），随后到一条迟到事件（窗口尚未彻底关闭）：
//   - end-1：(end-1)+5 > end，仍在允许延迟内 -> 接收并修正 Revision=2（sum=101）；
//   - end-6：(end-6)+5 = end-1 <= end，越过接收边界 -> 拒绝，即使窗口关闭线 end+5 未到；
//   - deep  ：落在窗口深处（远离 End），窗口越大它离 End 越远 -> 必须拒绝。
//     （旧实现按“窗口 End+允许延迟”判定，大窗口下会把它错误收进去——bug 就在这里。）
func TestAllowedLatenessNotScaledByWindow(t *testing.T) {
	cases := []struct {
		name     string
		winSize  time.Duration
		end      int // 窗口 End（秒）：10s 窗口=100，600s 窗口=600
		lateSec  int
		deep     bool
		wantLate bool
	}{
		{"小窗口-尾部内迟到end-1修正", 10 * time.Second, 100, 99, false, false},
		{"小窗口-超延迟end-6拒绝", 10 * time.Second, 100, 94, false, true},
		{"大窗口-尾部内迟到end-1修正", 10 * time.Minute, 600, 599, false, false},
		{"大窗口-超延迟end-6拒绝", 10 * time.Minute, 600, 594, false, true},
		{"大窗口-窗口深处@300必须拒绝", 10 * time.Minute, 600, 300, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			baseSec := tc.end - 5 // base 在触发前建立窗口状态（小窗口@95/大窗口@595）
			if tc.deep {
				baseSec = 300 // 深处用例：base 与迟到同处窗口深处
			}
			events := []Event{
				{ID: "base", Key: "k", Shard: 0, Offset: 0, EventTime: ts(baseSec), Value: 2},
				{ID: "end", Key: "g", Shard: 0, Offset: 1, EventTime: ts(tc.end), Value: 1}, // 推水位线到 End
				{ID: "stale", Key: "k", Shard: 0, Offset: 2, EventTime: ts(tc.lateSec), Value: 99},
			}
			cfg := Config{
				WindowSize: tc.winSize, AllowedLateness: 5 * time.Second,
				FetchBatch: 1, InFlightLimit: 2, CheckpointEvery: 100,
			}
			sink := NewMemSink()
			rt := newTestRuntime(t, [][]Event{events}, cfg, sink)
			if err := rt.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			st := rt.Stats()
			var winStart int
			if tc.winSize == 10*time.Minute {
				winStart = 0 // @9x/@300/@59x 都在 [0,600)
			} else {
				winStart = 90
			}
			id := outputID(&WindowState{Key: "k", Window: Window{
				Start: ts(winStart), End: ts(winStart + int(tc.winSize/time.Second))}})
			g, ok := sink.Get(id)
			if !ok {
				t.Fatalf("窗口结果缺失 id=%s, sink=%v", id, sink.Results())
			}
			if tc.wantLate {
				if st.LateDropped != 1 || st.Corrections != 0 {
					t.Fatalf("stats=%+v, want LateDropped=1 Corrections=0", st)
				}
				if g.Revision != 1 || g.Sum != 2 || g.Count != 1 {
					t.Fatalf("被拒迟到不得影响聚合: %+v, want rev=1 sum=2 count=1", g)
				}
			} else {
				if st.LateDropped != 0 || st.Corrections != 1 {
					t.Fatalf("stats=%+v, want LateDropped=0 Corrections=1", st)
				}
				if g.Revision != 2 || g.Sum != 101 || g.Count != 2 {
					t.Fatalf("允许延迟内应修正: %+v, want rev=2 sum=101 count=2", g)
				}
			}
		})
	}
}

// makeTwoShardLateStream 构造两分片流（窗口 10s / 允许延迟 10s 语义下使用）：
//   - t=0..9：两个分片各一条 key=k、v=1，共 20 条落在窗口 [0,10)；
//   - t=10..25：两个分片各一条 key=p 的齐步事件，把水位线平滑推过窗口关闭线；
//   - 在分片 1 的槽位注入三条迟到事件（槽位归属使其在处理时看到完整的当拍组合水位线，
//     且判定结果对 1s 的分片滞后不敏感）：
//   - t=12 槽：@9 v=3  -> 9+10=19 > 水位线，允许延迟内 -> 修正 Revision=2；
//   - t=14 槽：@2 v=7  -> 2+10=12 <= 水位线(>=13)，越界拒绝；
//   - t=18 槽：@1 v=5  -> 1+10=11 <= 水位线(>=17)，越界拒绝（窗口仍未彻底关闭）。
//
// 期望：窗口 [0,10)/k 最终 Revision=2、sum=23、count=21；LateDropped=2。
func makeTwoShardLateStream() [][]Event {
	sh := [][]Event{{}, {}}
	off := []uint64{0, 0}
	put := func(s, sec int, key string, v float64, id string) {
		sh[s] = append(sh[s], Event{ID: id, Key: key, Shard: s, Offset: off[s], EventTime: ts(sec), Value: v})
		off[s]++
	}
	for t := 0; t <= 25; t++ {
		if t < 10 {
			put(0, t, "k", 1, "b0-"+itoa(t))
			put(1, t, "k", 1, "b1-"+itoa(t))
			continue
		}
		put(0, t, "p", 1, "f0-"+itoa(t))
		switch t {
		case 12:
			put(1, 9, "k", 3, "late-9")
		case 14:
			put(1, 2, "k", 7, "late-2")
		case 18:
			put(1, 1, "k", 5, "late-1")
		default:
			put(1, t, "p", 1, "f1-"+itoa(t))
		}
	}
	return sh
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

type appliedRev struct {
	Revision int64
	Sum      float64
	Count    int
}

// TestLateBoundaryDeterministicAcrossShardingAndBatches：多分片并发、不同批次大小、
// 不同在途上限、多轮调度下，修正版本序列（版本号 + 每版累计值）与拒绝计数必须完全一致。
func TestLateBoundaryDeterministicAcrossShardingAndBatches(t *testing.T) {
	shards := makeTwoShardLateStream()
	const windowID = "0-10000000000/k"
	cfgs := []Config{
		{WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second, FetchBatch: 1, InFlightLimit: 2},
		{WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second, FetchBatch: 3, InFlightLimit: 8},
		{WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second, FetchBatch: 16, InFlightLimit: 64},
	}
	var golden []appliedRev
	var goldenLate int
	for ci, cfg := range cfgs {
		for rep := 0; rep < 4; rep++ {
			sink := NewMemSink()
			rt := NewRuntime(cfg, &SliceSource{Data: shards}, sink)
			if err := rt.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			var seq []appliedRev
			for _, o := range sink.Applied(windowID) {
				seq = append(seq, appliedRev{o.Revision, o.Sum, o.Count})
			}
			if golden == nil {
				golden = seq
				goldenLate = rt.Stats().LateDropped
			} else {
				if len(seq) != len(golden) {
					t.Fatalf("cfg#%d rep%d 修订序列长度=%d, golden=%d (%v vs %v)",
						ci, rep, len(seq), len(golden), seq, golden)
				}
				for i := range golden {
					if seq[i] != golden[i] {
						t.Fatalf("cfg#%d rep%d 修订序列不一致: %v vs golden %v", ci, rep, seq, golden)
					}
				}
				if rt.Stats().LateDropped != goldenLate {
					t.Fatalf("cfg#%d rep%d LateDropped=%d, golden=%d", ci, rep, rt.Stats().LateDropped, goldenLate)
				}
			}
		}
	}
	want := []appliedRev{{1, 20, 20}, {2, 23, 21}}
	if len(golden) != len(want) {
		t.Fatalf("修订序列=%v, want %v", golden, want)
	}
	for i := range want {
		if golden[i] != want[i] {
			t.Fatalf("修订序列=%v, want %v（越界迟到不得并入，允许延迟内迟到修正一次）", golden, want)
		}
	}
	if goldenLate != 2 {
		t.Fatalf("LateDropped=%d, want 2", goldenLate)
	}
	t.Logf("3 种批次/在途配置 × 4 轮调度，修订序列恒为 %v，LateDropped=%d", golden, goldenLate)
}

// TestLateBoundaryAfterRecovery：检查点恢复后两条边界与版本语义必须同时成立：
//   - 已确认的 Revision=1,2 不再给下游出现第二遍；修正进度（Fired/Revision）不倒退；
//   - 恢复后水位线被推到 15：@4 的接收截止线 4+10=14<=15，即使窗口 [0,10) 的关闭线
//     20 未到，也必须按迟到拒绝，不并入、不补发；
//   - @6：6+10=16>15，仍在允许延迟内 -> 在 Revision=2 的基础上继续修正为 Revision=3。
func TestLateBoundaryAfterRecovery(t *testing.T) {
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	mkEvents := func() []Event {
		es := make([]Event, 0)
		add := func(id string, key string, sec int, v float64) {
			es = append(es, Event{ID: id, Key: key, Shard: 0,
				Offset: uint64(len(es)), EventTime: ts(sec), Value: v})
		}
		add("a", "k", 1, 1)
		add("t", "k", 10, 0)     // 水位线 10：首次触发 Revision=1（sum=1）
		add("c", "k", 3, 3)      // 允许延迟内修正 Revision=2（sum=4）—— 处理满 3 条时落检查点
		add("p", "p", 15, 1)     // 恢复后：水位线 -> 15
		add("stale", "k", 4, 99) // 4+10=14<=15：越界拒绝（窗口关闭线 20 未到）
		add("late", "k", 6, 6)   // 6+10=16>15：允许延迟内，继续修正 Revision=3（sum=10）
		add("z", "p", 30, 0)     // 推过窗口关闭线 20
		return es
	}
	all := mkEvents()
	cfg := Config{
		WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second,
		FetchBatch: 1, InFlightLimit: 4, CheckpointEvery: 3,
		CheckpointStore: store,
	}
	cfg.Logger = func(f string, a ...any) { t.Logf(f, a...) }
	sink := NewMemSink()

	// 第一次运行：第 4 次 Fetch 崩溃，此时 a/t/c 已处理，Rev=1,2 已确认且 Rev=2 已落检查点。
	crash := &crashSource{inner: &SliceSource{Data: [][]Event{all}}, limit: 3}
	rt1 := NewRuntime(cfg, crash, sink)
	if err := rt1.Run(t.Context()); !errors.Is(err, errCrash) {
		t.Fatalf("应返回模拟崩溃, got %v", err)
	}
	if revs := sink.Revisions("0-10000000000/k"); len(revs) != 2 || revs[0] != 1 || revs[1] != 2 {
		t.Fatalf("崩溃前修订序列=%v, want [1 2]", revs)
	}

	// 恢复并跑完
	rt2 := NewRuntime(cfg, &SliceSource{Data: [][]Event{all}}, sink)
	if ok, err := rt2.Recover(t.Context()); !ok || err != nil {
		t.Fatalf("恢复失败: ok=%v err=%v", ok, err)
	}
	if err := rt2.Run(t.Context()); err != nil {
		t.Fatal(err)
	}

	revs := sink.Revisions("0-10000000000/k")
	want := []int64{1, 2, 3}
	if len(revs) != 3 {
		t.Fatalf("恢复后修订序列=%v, want %v（已确认的 1,2 不得重复出现，修正进度不倒退）", revs, want)
	}
	for i := range want {
		if revs[i] != want[i] {
			t.Fatalf("恢复后修订序列=%v, want %v", revs, want)
		}
	}
	got, _ := sink.Get("0-10000000000/k")
	if got.Sum != 10 || got.Count != 3 {
		t.Fatalf("最终 sum=%v count=%d, want 10/3（越界 stale 不得并入，允许延迟内 late 接续修正）", got.Sum, got.Count)
	}
	if st := rt2.Stats(); st.LateDropped != 1 || st.Corrections != 1 {
		t.Fatalf("恢复后 stats=%+v, want LateDropped=1 Corrections=1（Rev=3 一条）", st)
	}
	t.Logf("恢复后边界语义正确: 修订=%v finalSum=%v count=%d sink.Write=%d(含幂等重放)",
		revs, got.Sum, got.Count, sink.Writes)
}
