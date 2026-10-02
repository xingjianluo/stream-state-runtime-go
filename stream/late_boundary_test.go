package stream

import (
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// 复现“允许延迟被窗口大小放大”的边界：
// 窗口 10s、允许延迟 5s，窗口 [0,10) 的彻底关闭线为 15。
//
//	@1 v=1       ：聚合，水位线低，不触发
//	@10 v=0      ：水位线 10，首次触发 Revision=1（sum=1,count=1）
//	@7 v=2（迟到）：水位线 10；7+5=12 > 10，仍在允许延迟内 -> Revision=2（sum=3,count=2）
//	@11 v=0      ：水位线推进到 11（窗口关闭线 15 未到，窗口仍开着）
//	@6 v=100（迟到）：6+5=11 <= 11，落后水位线已达到允许延迟上界 -> 必须按迟到拒绝
//	                 （旧实现按窗口关闭线 15 判定，会把它错误收进窗口并补发修正）
//	@7 v=4（迟到）：7+5=12 > 11，仍在允许延迟内 -> Revision=3（sum=7,count=3）
//	@15 v=0      ：水位线越过关闭线 15，窗口彻底关闭清理
func lateBoundaryEvents() []Event {
	ev := func(id string, off uint64, sec int, v float64) Event {
		return Event{ID: id, Key: "k", Shard: 0, Offset: off, EventTime: ts(sec), Value: v}
	}
	return []Event{
		ev("a", 0, 1, 1),
		ev("t10", 1, 10, 0),
		ev("late-in-1", 2, 7, 2),
		ev("t11", 3, 11, 0),
		ev("late-out", 4, 6, 100),
		ev("late-in-2", 5, 7, 4),
		ev("z", 6, 15, 0),
	}
}

func lateBoundaryCfg() Config {
	return Config{
		WindowSize: 10 * time.Second, AllowedLateness: 5 * time.Second,
		FetchBatch: 1, InFlightLimit: 4, CheckpointEvery: 100,
	}
}

// TestLateBeyondAllowedLatenessRejectedWhileWindowOpen：
// 先让水位线超过“事件时间 + 允许延迟”，再送一条属于尚未彻底关闭窗口的旧事件，
// 必须按迟到拒绝：统计计 LateDropped、不计 Processed，聚合与下游结果都不受影响，不产生新修正。
func TestLateBeyondAllowedLatenessRejectedWhileWindowOpen(t *testing.T) {
	events := lateBoundaryEvents()
	cfg := lateBoundaryCfg()
	cfg.Logger = func(f string, a ...any) { t.Logf(f, a...) }
	sink := NewMemSink()
	rt := newTestRuntime(t, [][]Event{events}, cfg, sink)
	if err := rt.Run(t.Context()); err != nil {
		t.Fatal(err)
	}

	st := rt.Stats()
	t.Logf("stats: %+v", st)
	if st.LateDropped != 1 {
		t.Fatalf("LateDropped=%d, want 1（超过允许延迟的旧事件必须拒绝，即使窗口尚未彻底关闭）", st.LateDropped)
	}
	if st.Processed != 6 {
		t.Fatalf("Processed=%d, want 6（被拒事件不计入处理）", st.Processed)
	}

	const id = "0-10000000000/k"
	got, ok := sink.Get(id)
	if !ok {
		t.Fatal("窗口 [0,10) 应产出结果")
	}
	if got.Sum != 7 || got.Count != 3 {
		t.Fatalf("被拒事件不得影响聚合: sum=%v count=%d, want sum=7 count=3", got.Sum, got.Count)
	}
	revs := sink.Revisions(id)
	wantRevs := []int64{1, 2, 3}
	if len(revs) != len(wantRevs) {
		t.Fatalf("修订序列=%v, want %v（被拒事件不得补发修正）", revs, wantRevs)
	}
	for i := range wantRevs {
		if revs[i] != wantRevs[i] {
			t.Fatalf("修订序列=%v, want %v", revs, wantRevs)
		}
	}
	if st.Corrections != 2 {
		t.Fatalf("Corrections=%d, want 2", st.Corrections)
	}
	// 最终结果与“只含被接受事件”的一次性聚合一致
	accepted := []Event{}
	for _, e := range events {
		if e.ID != "late-out" {
			accepted = append(accepted, e)
		}
	}
	assertConsistent(t, resultsMap(sink), reference([][]Event{accepted}, cfg.WindowSize))
}

// 构造“驱动/伴随”双分片流，使修正序列对任意批次大小、在途上限与调度都确定：
//   - 驱动分片 shard0 承载全部早期数据、水位线标记与迟到数据（两个键）；
//   - 伴随分片 shard1 只放“始终不落后于驱动线”的标记（独立键 w，不参与目标窗口），
//     于是组合水位线严格等于驱动分片的水位线。
//
// 加“每分片单积压槽位”的归并约束：窗口 [0,10) 的触发（WM=10，需 t10 与 w10 都处理）
// 必然发生在任何 @8 迟到事件之前；@11 推进（t11+w11）必然发生在 @6 越界事件之前。
//
//	driver @1,@2,@3（k0/k1 早期数据）
//	@10（t10+w10）：首次触发，两键 Revision=1
//	@8  ×2 键      ：8+5=13>10，允许延迟内迟到 -> Revision=2
//	@11（t11+w11）：水位线 11（窗口关闭线 15 未到）
//	@6  ×2 键      ：6+5=11<=11，越界迟到 -> 两个键各拒绝 1 条
//	@8  ×2 键      ：仍在允许延迟内 -> Revision=3
//	@15（t15+w15）：越过关闭线，窗口彻底关闭清理
func pairedShards() [][]Event {
	driver, standby := []Event{}, []Event{}
	var doff, soff uint64
	dadd := func(sec int, key string, v float64) {
		driver = append(driver, Event{
			ID: "d" + itoa(sec) + "-" + key + "-" + itoa(int(doff)), Key: key, Shard: 0,
			Offset: doff, EventTime: ts(sec), Value: v,
		})
		doff++
	}
	sadd := func(sec int) {
		standby = append(standby, Event{
			ID: "w" + itoa(sec) + "-" + itoa(int(soff)), Key: "w", Shard: 1,
			Offset: soff, EventTime: ts(sec), Value: 0,
		})
		soff++
	}
	for sec := 1; sec <= 3; sec++ {
		dadd(sec, "k0", float64(sec))
		dadd(sec, "k1", float64(sec*10))
	}
	sadd(10)
	dadd(10, "k0", 0)
	dadd(8, "k0", 8)
	dadd(8, "k1", 80)
	// @11 与 @8 的批次：每个驱动时间点都配一条同时刻 standby 标记，
	// 保证无论批次大小/调度如何，处理驱动事件时组合水位线恰好等于该驱动时间点
	// （standby 永不领先也永不落后于驱动线）：@6 在 WM=11 稳定越界；
	// 第二批 @8 在 WM=11（8+5=13>11）稳定接收为 Revision=3。
	sadd(11)
	dadd(11, "k0", 0)
	dadd(6, "k0", 1000) // 越界迟到
	dadd(6, "k1", 1000) // 越界迟到
	sadd(8)
	dadd(8, "k0", 8)
	dadd(8, "k1", 80)
	sadd(15)
	dadd(15, "k0", 0)
	return [][]Event{driver, standby}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		b[n] = '-'
	}
	return string(b[n:])
}

// TestLateBoundaryMultiShardDeterministic：多分片并发、不同 FetchBatch /
// InFlightLimit / CheckpointEvery（改变在途与调度形态）下，
// 每个 (窗口,键) 的修正版本序列必须完全一致；越界迟到稳定拒绝，结果不受影响。
func TestLateBoundaryMultiShardDeterministic(t *testing.T) {
	shards := pairedShards()
	configs := []Config{
		{WindowSize: 10 * time.Second, AllowedLateness: 5 * time.Second,
			FetchBatch: 1, InFlightLimit: 2, CheckpointEvery: 3},
		{WindowSize: 10 * time.Second, AllowedLateness: 5 * time.Second,
			FetchBatch: 4, InFlightLimit: 8, CheckpointEvery: 5},
		{WindowSize: 10 * time.Second, AllowedLateness: 5 * time.Second,
			FetchBatch: 16, InFlightLimit: 64, CheckpointEvery: 100},
		{WindowSize: 10 * time.Second, AllowedLateness: 5 * time.Second,
			FetchBatch: 2, InFlightLimit: 1, CheckpointEvery: 7},
	}
	const (
		id0 = "0-10000000000/k0"
		id1 = "0-10000000000/k1"
	)
	wantRevs := []int64{1, 2, 3}
	var first map[string][]int64
	for ci, cfg := range configs {
		for rep := 0; rep < 3; rep++ { // 每种配置多遍，覆盖不同调度时序
			sink := NewMemSink()
			rt := NewRuntime(cfg, &SliceSource{Data: shards}, sink)
			if err := rt.Run(t.Context()); err != nil {
				t.Fatal(err)
			}
			got := map[string][]int64{id0: sink.Revisions(id0), id1: sink.Revisions(id1)}
			if first == nil {
				first = got
			} else {
				for id, w := range first {
					g := got[id]
					if len(g) != len(w) {
						t.Fatalf("cfg#%d rep=%d %s 修订序列长度不一致: %v vs %v", ci, rep, id, g, w)
					}
					for i := range w {
						if g[i] != w[i] {
							t.Fatalf("cfg#%d rep=%d %s 修订序列不一致: %v vs %v", ci, rep, id, g, w)
						}
					}
				}
			}
			for id, revs := range got {
				if len(revs) != len(wantRevs) {
					t.Fatalf("cfg#%d rep=%d %s 修订序列=%v, want %v（越界迟到不得补发修正）",
						ci, rep, id, revs, wantRevs)
				}
				for i := range wantRevs {
					if revs[i] != wantRevs[i] {
						t.Fatalf("cfg#%d rep=%d %s 修订序列=%v, want %v", ci, rep, id, revs, wantRevs)
					}
				}
			}
			st := rt.Stats()
			if st.LateDropped != 2 {
				t.Fatalf("cfg#%d rep=%d LateDropped=%d, want 2（两个键各一条越界迟到）",
					ci, rep, st.LateDropped)
			}
			if st.Corrections != 4 {
				t.Fatalf("cfg#%d rep=%d Corrections=%d, want 4（两键各 2 次修正）", ci, rep, st.Corrections)
			}
			// 终值：k0 = 1+2+3+8+8 = 22（5 条）；k1 = 10+20+30+80+80 = 220（5 条）；
			// 两条 value=1000 的越界迟到不得计入。
			g0, _ := sink.Get(id0)
			g1, _ := sink.Get(id1)
			if g0.Sum != 22 || g0.Count != 5 {
				t.Fatalf("cfg#%d rep=%d k0 结果=%v count=%d, want sum=22 count=5", ci, rep, g0.Sum, g0.Count)
			}
			if g1.Sum != 220 || g1.Count != 5 {
				t.Fatalf("cfg#%d rep=%d k1 结果=%v count=%d, want sum=220 count=5", ci, rep, g1.Sum, g1.Count)
			}
			// 越界事件所属窗口最终正常清理，去重与窗口状态末态归零
			if wins, dedup := rt.StateSizes(); wins != 0 || dedup != 0 {
				t.Fatalf("cfg#%d rep=%d 末态状态应归零: 窗口=%d 去重=%d", ci, rep, wins, dedup)
			}
		}
	}
	t.Logf("4 种配置 ×3 遍修正序列完全一致: k0=%v k1=%v", first[id0], first[id1])
}

// TestLateBoundaryRecovery：崩溃恢复后迟到边界语义必须完整保留：
//   - 恢复出的水位线继续按“事件时间+允许延迟”判定：越界旧事件（窗口仍开着）照样拒绝；
//   - 允许延迟内的迟到数据在已持久化的 Revision 上继续修正（不倒退、不重复）；
//   - 已确认的 Revision=1 不再重放；崩溃时未确认的 Revision=2 恢复后重放且只生效一次；
//   - 恢复的去重记录仍能认出崩溃前处理过的事件；最终结果与“只含被接受事件”的一次性聚合一致。
func TestLateBoundaryRecovery(t *testing.T) {
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	// 窗口 [0,10)，触发线 10、关闭线 15、事件级迟到线 = 事件时间+5。
	allEvents := []Event{}
	add := func(id string, sec int, v float64) {
		allEvents = append(allEvents, Event{ID: id, Key: "k", Shard: 0,
			Offset: uint64(len(allEvents)), EventTime: ts(sec), Value: v})
	}
	add("a", 1, 1)
	add("b", 2, 2)
	add("t10", 10, 0) // 首次触发 Revision=1（sum=3）；崩溃前已确认
	add("c", 8, 8)    // Revision=2（sum=11,count=3）；崩溃时未确认，随检查点持久化
	// —— 崩溃点（4 次 Fetch 后）——
	add("t11", 11, 0)                                                // 恢复后水位线 11
	add("out", 6, 100)                                               // 6+5=11<=11：越界迟到（关闭线 15 未到）必须拒绝
	allEvents = append(allEvents, Event{ID: "c", Key: "k", Shard: 0, // c 的重投递：恢复的去重记录必须认出
		Offset: uint64(len(allEvents)), EventTime: ts(8), Value: 7})
	add("d", 8, 4)  // 8+5=13>11：允许延迟内迟到 -> Revision=3（sum=15,count=4）
	add("z", 15, 0) // 越过关闭线 15，窗口彻底关闭清理

	// CheckpointEvery=3：t10(第 3 条)处理后检查点确认 Revision=1；
	// c(第 4 条)产生 Revision=2 并冲刷，随后崩溃（再做一次检查点持久化全部进度）。
	// “崩溃时未确认输出的重放”由 TestCorrectionRecovery 专门覆盖；
	// 本测试聚焦：恢复出的水位线/去重/修订进度与新的事件级迟到边界。
	cfg := Config{
		WindowSize: 10 * time.Second, AllowedLateness: 5 * time.Second,
		FetchBatch: 1, InFlightLimit: 4, CheckpointEvery: 3,
		CheckpointStore: store,
	}
	cfg.Logger = func(f string, a ...any) { t.Logf(f, a...) }
	sink := NewMemSink()

	// —— 崩溃点（4 次 Fetch 后）：Revision=1 已随 seq=2 确认；
	//    c 产生的 Revision=2 已写入 Sink 并冲刷，但下一次检查点（计数=4 时触发）
	//    与崩溃的先后不可控——Run 在 Fetch 失败后统一再做一次检查点，因此
	//    seq=3 一定落盘且 pending 为空；Revision=2 由 Sink 幂等兜底，不重复生效。
	crash := &crashSource{inner: &SliceSource{Data: [][]Event{allEvents}}, limit: 4}
	rt1 := NewRuntime(cfg, crash, sink)
	if err := rt1.Run(t.Context()); !errors.Is(err, errCrash) {
		t.Fatalf("应返回模拟崩溃错误, got %v", err)
	}
	t.Logf("崩溃前生效修订=%v", sink.Revisions("0-10000000000/k"))

	rt2 := NewRuntime(cfg, &SliceSource{Data: [][]Event{allEvents}}, sink)
	if ok, err := rt2.Recover(t.Context()); err != nil || !ok {
		t.Fatalf("恢复失败: ok=%v err=%v", ok, err)
	}
	if err := rt2.Run(t.Context()); err != nil {
		t.Fatal(err)
	}

	const id = "0-10000000000/k"
	revs := sink.Revisions(id)
	want := []int64{1, 2, 3}
	if len(revs) != len(want) {
		t.Fatalf("恢复后修订序列=%v, want %v（已确认不重放、未确认只生效一次、进度不倒退）", revs, want)
	}
	for i := range want {
		if revs[i] != want[i] {
			t.Fatalf("恢复后修订序列=%v, want %v", revs, want)
		}
	}
	got, ok := sink.Get(id)
	if !ok || got.Sum != 15 || got.Count != 4 {
		t.Fatalf("最终结果 %+v ok=%v, want sum=15 count=4", got, ok)
	}
	st := rt2.Stats()
	if st.LateDropped != 1 {
		t.Fatalf("恢复后越界迟到 LateDropped=%d, want 1", st.LateDropped)
	}
	if st.Duplicates != 1 {
		t.Fatalf("恢复的去重记录应认出崩溃前事件, Duplicates=%d, want 1", st.Duplicates)
	}
	if st.Corrections != 1 {
		t.Fatalf("恢复后仅允许延迟内迟到 d 产生一次修正, Corrections=%d, want 1", st.Corrections)
	}
	// 与“只含被接受事件（去掉越界 out 与重复 dup-c）”的一次性聚合一致
	accepted := []Event{}
	for _, e := range allEvents {
		if e.ID == "out" {
			continue
		}
		accepted = append(accepted, e)
	}
	// 参考实现本身会对同 ID 去重（WindowManager 不去重，需要手工去掉重复的 c）：
	deduped := []Event{}
	seen := map[string]bool{}
	for _, e := range accepted {
		if seen[e.ID] {
			continue
		}
		seen[e.ID] = true
		deduped = append(deduped, e)
	}
	assertConsistent(t, resultsMap(sink), reference([][]Event{deduped}, cfg.WindowSize))
	// 末态状态归零，回收边界没有因修复变松
	if wins, dedup := rt2.StateSizes(); wins != 0 || dedup != 0 {
		t.Fatalf("结束后状态应归零: 窗口=%d 去重=%d", wins, dedup)
	}
	t.Logf("恢复后迟到边界正确: 修订=%v finalSum=%v count=%d late=%d dup=%d",
		revs, got.Sum, got.Count, st.LateDropped, st.Duplicates)
}
