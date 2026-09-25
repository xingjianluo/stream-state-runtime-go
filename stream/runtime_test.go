package stream

import (
	"fmt"
	"sort"
	"testing"
	"time"
)

// 构造两分片乱序事件流：shard 内事件时间基本递增，shard 间交错。
func makeStream(n int) [][]Event {
	shards := make([][]Event, 2)
	for i := 0; i < n; i++ {
		shard := i % 2
		// 制造乱序：偶数分片时间正常，奇数分片时间滞后 3 秒
		sec := i*2 - shard*3
		if sec < 0 {
			sec = 0
		}
		ev := Event{
			ID:        fmt.Sprintf("e%d", i),
			Key:       fmt.Sprintf("k%d", i%3),
			Shard:     shard,
			Offset:    uint64(len(shards[shard])),
			EventTime: ts(sec),
			Value:     float64(i),
		}
		shards[shard] = append(shards[shard], ev)
	}
	return shards
}

// 参考实现：直接对全量事件分组聚合（不考虑迟到），作为一致性基准。
func reference(shards [][]Event, windowSize time.Duration) map[string]Output {
	mgr := NewWindowManager(windowSize, 0)
	for _, events := range shards {
		for _, ev := range events {
			mgr.Add(ev, time.Time{})
		}
	}
	outs := mgr.FireReady(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
	ref := make(map[string]Output, len(outs))
	for _, o := range outs {
		ref[o.ID] = o
	}
	return ref
}

func newTestRuntime(t *testing.T, shards [][]Event, cfg Config, sink *MemSink) *Runtime {
	t.Helper()
	cfg.Logger = func(format string, args ...any) { t.Logf(format, args...) }
	return NewRuntime(cfg, &SliceSource{Data: shards}, sink)
}

func resultsMap(sink *MemSink) map[string]Output {
	m := make(map[string]Output)
	for _, o := range sink.Results() {
		m[o.ID] = o
	}
	return m
}

func assertConsistent(t *testing.T, got map[string]Output, ref map[string]Output) {
	t.Helper()
	if len(got) != len(ref) {
		t.Fatalf("输出窗口数不一致: got=%d want=%d", len(got), len(ref))
	}
	ids := make([]string, 0, len(ref))
	for id := range ref {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		g, r := got[id], ref[id]
		if g.Sum != r.Sum || g.Count != r.Count {
			t.Fatalf("窗口 %s 结果不一致: got sum=%v count=%d, want sum=%v count=%d",
				id, g.Sum, g.Count, r.Sum, r.Count)
		}
	}
}

func TestOutOfOrderConsistency(t *testing.T) {
	shards := makeStream(60)
	cfg := Config{WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second, InFlightLimit: 8, FetchBatch: 4, CheckpointEvery: 10}
	sink := NewMemSink()
	rt := newTestRuntime(t, shards, cfg, sink)
	if err := rt.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	st := rt.Stats()
	t.Logf("stats: %+v", st)
	if st.LateDropped != 0 {
		t.Fatalf("允许延迟足够大时不应有迟到丢弃, got %d", st.LateDropped)
	}
	assertConsistent(t, resultsMap(sink), reference(shards, cfg.WindowSize))
}

func TestDuplicateEvents(t *testing.T) {
	shards := makeStream(40)
	// 每个事件重复投递一次（模拟 at-least-once 上游）
	for s := range shards {
		dup := make([]Event, 0, len(shards[s])*2)
		for _, ev := range shards[s] {
			dup = append(dup, ev)
			ev2 := ev
			ev2.Offset = uint64(len(dup)) // 重投递占据新位点
			dup = append(dup, ev2)
		}
		// 修正位点为连续下标
		for i := range dup {
			dup[i].Offset = uint64(i)
		}
		shards[s] = dup
	}
	cfg := Config{WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second, CheckpointEvery: 7}
	sink := NewMemSink()
	rt := newTestRuntime(t, shards, cfg, sink)
	if err := rt.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	st := rt.Stats()
	t.Logf("stats: %+v (重复投递 40 次应全部被去重)", st)
	if st.Duplicates != 40 {
		t.Fatalf("Duplicates=%d, want 40", st.Duplicates)
	}
	if st.Processed != 40 {
		t.Fatalf("Processed=%d, want 40", st.Processed)
	}
	assertConsistent(t, resultsMap(sink), reference(makeStream(40), cfg.WindowSize))
}

func TestLateEventDropped(t *testing.T) {
	// 单分片：先推进水位线，再投递超出允许延迟的迟到事件
	events := []Event{
		{ID: "a", Key: "k", Shard: 0, Offset: 0, EventTime: ts(100), Value: 1},
		{ID: "b", Key: "k", Shard: 0, Offset: 1, EventTime: ts(101), Value: 2},
		{ID: "late", Key: "k", Shard: 0, Offset: 2, EventTime: ts(1), Value: 99},
	}
	cfg := Config{WindowSize: 10 * time.Second, AllowedLateness: 5 * time.Second}
	sink := NewMemSink()
	rt := newTestRuntime(t, [][]Event{events}, cfg, sink)
	if err := rt.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	st := rt.Stats()
	t.Logf("stats: %+v", st)
	if st.LateDropped != 1 {
		t.Fatalf("LateDropped=%d, want 1", st.LateDropped)
	}
	for _, o := range sink.Results() {
		if o.Sum == 99 || o.Count > 2 && o.Window.Start == ts(0) {
			t.Fatalf("迟到事件被计入聚合: %+v", o)
		}
	}
}

func TestDeterministicAcrossRuns(t *testing.T) {
	shards := makeStream(80)
	cfg := Config{WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second, InFlightLimit: 4, FetchBatch: 3}
	var runs []map[string]Output
	for i := 0; i < 3; i++ {
		sink := NewMemSink()
		rt := newTestRuntime(t, shards, cfg, sink)
		if err := rt.Run(t.Context()); err != nil {
			t.Fatal(err)
		}
		runs = append(runs, resultsMap(sink))
	}
	for i := 1; i < len(runs); i++ {
		assertConsistent(t, runs[i], runs[0])
	}
	t.Logf("3 次并发运行结果完全一致, 窗口数=%d", len(runs[0]))
}
