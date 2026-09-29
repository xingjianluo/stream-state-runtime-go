package stream

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// gatedSource 每次只返回 gate 通道放行的一条事件，用于精确控制水位线推进
// 与迟到数据的相对时机。
type gatedSource struct {
	events   []Event
	gate     chan struct{}
	abort    chan struct{}
	abortErr error
}

func newGatedSource(events []Event) *gatedSource {
	return &gatedSource{events: events, gate: make(chan struct{}, 1), abort: make(chan struct{})}
}

func (g *gatedSource) Shards() int { return 1 }

func (g *gatedSource) Fetch(ctx context.Context, shard int, after uint64, max int) ([]Event, error) {
	if int(after) >= len(g.events) {
		return nil, nil // 分片耗尽
	}
	select {
	case <-g.gate:
		return []Event{g.events[after]}, nil
	case <-g.abort:
		return nil, g.abortErr
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// deliver 放行一条事件并等待其被完整判定（处理/重复/迟到均会推进计数）。
func (g *gatedSource) deliver(ctx context.Context, rt *Runtime) error {
	rt.mu.Lock()
	before := rt.stats.Processed + rt.stats.Duplicates + rt.stats.LateDropped
	rt.mu.Unlock()
	g.gate <- struct{}{}
	deadline := time.Now().Add(5 * time.Second)
	for {
		rt.mu.Lock()
		n := rt.stats.Processed + rt.stats.Duplicates + rt.stats.LateDropped
		rt.mu.Unlock()
		if n > before {
			return nil
		}
		if !time.Now().Before(deadline) {
			t := ctx.Err()
			return fmt.Errorf("等待事件处理超时: %w", t)
		}
		time.Sleep(time.Millisecond)
	}
}

func outID(windowStartSec int, key string) string {
	return outputID(Window{Start: ts(windowStartSec), End: ts(windowStartSec + 10)}, key)
}

// TestLateCorrectionsConverge 验证同一窗口在首次结果已发给下游后，
// 多次容忍期内迟到数据产生版本递增的修正，下游逐版更新、重试不重复，
// 最终收敛到"全部数据一次性算完"的结果。
func TestLateCorrectionsConverge(t *testing.T) {
	const windowSize = 10 * time.Second
	const lateness = 5 * time.Second
	events := []Event{
		// 窗口 [0,10) 的首批数据（此时水位线为零）
		{ID: "a", Key: "k", Shard: 0, EventTime: ts(1), Value: 1},
		{ID: "b", Key: "k", Shard: 0, EventTime: ts(2), Value: 2},
		// 心跳把水位线推到 10，使窗口 [0,10) 首次触发
		{ID: "h1", Key: "hb", Shard: 0, EventTime: ts(10), Value: 0},
		// 第二条心跳把水位线推到 11（cutoff=6）
		{ID: "h2", Key: "hb", Shard: 0, EventTime: ts(11), Value: 0},
		// 迟到数据 1：eventTime=7 属于 [0,10)，在容忍期内 -> 修正 rev=2
		{ID: "late1", Key: "k", Shard: 0, EventTime: ts(7), Value: 10},
		// 重复投递 late1：不得产生新版本、不得重复计入
		{ID: "late1dup", Key: "k", Shard: 0, EventTime: ts(7), Value: 10},
		// 水位线推到 13（cutoff=8）
		{ID: "h3", Key: "hb", Shard: 0, EventTime: ts(13), Value: 0},
		// 迟到数据 2：eventTime=9 -> 修正 rev=3
		{ID: "late2", Key: "k", Shard: 0, EventTime: ts(9), Value: 20},
		// 超出容忍：eventTime=8 <= cutoff=8，拒绝且不产生修正
		{ID: "toolate", Key: "k", Shard: 0, EventTime: ts(8), Value: 100},
	}
	// 修正：dup 复用同一事件 ID 才是真正的重复投递
	events[5].ID = "late1"
	for i := range events {
		events[i].Offset = uint64(i)
	}

	src := newGatedSource(events)
	sink := NewMemSink()
	cfg := Config{WindowSize: windowSize, AllowedLateness: lateness, FetchBatch: 1}
	cfg.Logger = func(format string, args ...any) { t.Logf(format, args...) }
	rt := NewRuntime(cfg, src, sink)

	runErr := make(chan error, 1)
	go func() { runErr <- rt.Run(context.Background()) }()

	id := outID(0, "k")
	flush := func(stage string) {
		t.Helper()
		if err := rt.flushPending(context.Background()); err != nil {
			t.Fatalf("%s: flush: %v", stage, err)
		}
	}

	// 依次处理 a、b、h1（h1 到达后窗口首次触发）
	for i := 0; i < 3; i++ {
		if err := src.deliver(context.Background(), rt); err != nil {
			t.Fatal(err)
		}
	}
	flush("首次触发后")
	if rev := sink.Revision(id); rev != 1 {
		t.Fatalf("首次触发后下游版本=%d, want 1", rev)
	}
	if r := sink.seen[id]; r.Sum != 3 || r.Count != 2 {
		t.Fatalf("首次结果错误: %+v", r)
	}

	// h2 推水位线到 11
	if err := src.deliver(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
	// 迟到数据 1（eventTime=7, cutoff=6）-> rev=2
	if err := src.deliver(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
	// 模拟同版本重试：先冲刷成功，再让 pending 里没有重复（已被确认过滤）。
	flush("修正1")
	if rev := sink.Revision(id); rev != 2 {
		t.Fatalf("第一次修正后版本=%d, want 2", rev)
	}
	if r := sink.seen[id]; r.Sum != 13 || r.Count != 3 {
		t.Fatalf("第一次修正结果错误: %+v", r)
	}

	// 重复投递（第 5 条）：不得产生新版本
	if err := src.deliver(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
	if rev := sink.Revision(id); rev != 2 {
		t.Fatalf("重复投递后版本=%d, 应保持 2", rev)
	}

	// 水位线 13（第 6 条），迟到数据 2（第 7 条）-> rev=3
	if err := src.deliver(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
	if err := src.deliver(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
	flush("修正2")
	if rev := sink.Revision(id); rev != 3 {
		t.Fatalf("第二次修正后版本=%d, want 3", rev)
	}
	if r := sink.seen[id]; r.Sum != 33 || r.Count != 4 {
		t.Fatalf("第二次修正结果错误: %+v", r)
	}

	// 超容忍事件（第 8 条）被拒绝，版本不变
	if err := src.deliver(context.Background(), rt); err != nil {
		t.Fatal(err)
	}
	if rev := sink.Revision(id); rev != 3 {
		t.Fatalf("超容忍事件后版本=%d, 应保持 3", rev)
	}

	if err := <-runErr; err != nil {
		t.Fatal(err)
	}
	// 结束后最终结果与"所有可接受数据一次性算完"一致：sum=1+2+10+20=33,count=4
	if r := sink.seen[id]; r.Sum != 33 || r.Count != 4 {
		t.Fatalf("最终收敛结果错误: %+v", r)
	}
	if len(sink.Results()) != 2 { // k（窗口0）与 hb（窗口0+1）
		t.Fatalf("下游唯一键数: got %d, want 2", len(sink.Results()))
	}
	// 同一 ID 在下游只有一份最新结果，不存在重复/累加
	st := rt.Stats()
	t.Logf("收敛验证: 最终 rev=%d sum=%v count=%d corrections=%d duplicates=%d late=%d",
		sink.seen[id].Revision, sink.seen[id].Sum, sink.seen[id].Count,
		st.Corrections, st.Duplicates, st.LateDropped)
	if st.Corrections != 2 || st.Duplicates != 1 || st.LateDropped != 1 {
		t.Fatalf("统计不符: %+v", st)
	}
	// 与参考实现逐窗口一致
	refEvents := []Event{
		{ID: "a", Key: "k", EventTime: ts(1), Value: 1},
		{ID: "b", Key: "k", EventTime: ts(2), Value: 2},
		{ID: "late1", Key: "k", EventTime: ts(7), Value: 10},
		{ID: "late2", Key: "k", EventTime: ts(9), Value: 20},
		{ID: "h1", Key: "hb", EventTime: ts(10), Value: 0},
		{ID: "h2", Key: "hb", EventTime: ts(11), Value: 0},
		{ID: "h3", Key: "hb", EventTime: ts(13), Value: 0},
	}
	assertConsistent(t, resultsMap(sink), reference([][]Event{refEvents}, windowSize))
}

// TestSinkRevisionSemantics 直接验证下游 upsert 语义：
// 同版本幂等、高版本覆盖、旧版本拒绝。
func TestSinkRevisionSemantics(t *testing.T) {
	sink := NewMemSink()
	w := Window{Start: ts(0), End: ts(10)}
	write := func(rev, sum int) error {
		return sink.Write(context.Background(), Output{ID: "w/k", Key: "k", Window: w, Sum: float64(sum), Count: rev, Revision: rev})
	}
	if err := write(1, 10); err != nil {
		t.Fatal(err)
	}
	if err := write(1, 10); err != nil { // 重试：幂等
		t.Fatal(err)
	}
	if sink.Writes != 2 || sink.StaleRejects != 0 {
		t.Fatalf("同版本重试验证失败: writes=%d stale=%d", sink.Writes, sink.StaleRejects)
	}
	if err := write(2, 15); err != nil {
		t.Fatal(err)
	}
	if r := sink.seen["w/k"]; r.Sum != 15 || r.Revision != 2 {
		t.Fatalf("新版本应覆盖: %+v", r)
	}
	if err := write(1, 10); err != nil { // 旧版本晚到
		t.Fatal(err)
	}
	if r := sink.seen["w/k"]; r.Sum != 15 || r.Revision != 2 {
		t.Fatalf("旧版本不得覆盖新值: %+v", r)
	}
	if sink.StaleRejects != 1 {
		t.Fatalf("旧版本应被计入 StaleRejects, got %d", sink.StaleRejects)
	}
	if len(sink.Results()) != 1 {
		t.Fatalf("下游每个 ID 只能有一份结果, got %d", len(sink.Results()))
	}
	t.Logf("下游版本语义验证通过: 同版本幂等/高版本覆盖/旧版本拒绝")
}
