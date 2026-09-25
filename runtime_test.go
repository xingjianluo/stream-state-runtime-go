package streamruntime

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// discardLogger 把关键日志转接到 testing 日志，便于复现审计。
func testLogger(t *testing.T) func(string, ...any) {
	return func(format string, args ...any) {
		t.Helper()
		t.Logf(format, args...)
	}
}

// TestE2EOutOfOrderAndDedup 单分片：乱序事件 + 重复投递，验证聚合结果与拒绝原因。
func TestE2EOutOfOrderAndDedup(t *testing.T) {
	cfg, err := NewConfig(1, time.Minute, 30*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	store := NewFileStore(filepath.Join(dir, "cp.bin"))
	sink := NewTestSink()
	rt := New(cfg, store, sink)
	rt.SetLogger(testLogger(t))

	var rejects []RejectedEvent
	rt.SetRejectObserver(func(re RejectedEvent) { rejects = append(rejects, re) })

	shard := NewMemShard(0)
	// 窗口 [00:00,01:00)，乱序进入。
	shard.AddEvent(Event{ID: "e2", Offset: 1, Key: "a", EventTime: sec(30), Value: 20})
	shard.AddEvent(Event{ID: "e1", Offset: 2, Key: "a", EventTime: sec(10), Value: 5})
	shard.AddEvent(Event{ID: "e3", Offset: 3, Key: "b", EventTime: sec(40), Value: 7})
	// 重复投递 e1（可能来自上游 at-least-once）。
	shard.AddEvent(Event{ID: "e1", Offset: 4, Key: "a", EventTime: sec(10), Value: 5})
	// 推进源端水位线越过窗口结束，触发窗口；随后关闭分片。
	shard.SetSourceWatermark(sec(70))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx, []ShardReader{shard}) }()

	// 等窗口触发后再关闭，保证关闭发生在处理过程中而非之前。
	waitFor(t, func() bool { return len(sink.Records()) >= 2 }, 2*time.Second)
	shard.Close()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	recs := sink.Records()
	// 期望窗口对 a、b 各一条：a 仅计 2 条（sum=25，重复 e1 不计入），b 1 条 sum=7。
	var sumA, sumB int64
	var cntA, cntB int64
	for _, r := range recs {
		if r.Key == "a" {
			sumA, cntA = r.Sum, r.Count
		}
		if r.Key == "b" {
			sumB, cntB = r.Sum, r.Count
		}
	}
	if cntA != 2 || sumA != 25 {
		t.Fatalf("key a: count=%d sum=%d, want 2/25; records=%+v", cntA, sumA, recs)
	}
	if cntB != 1 || sumB != 7 {
		t.Fatalf("key b: count=%d sum=%d, want 1/7", cntB, sumB)
	}
	if dups := sink.Duplicates(); len(dups) != 0 {
		t.Fatalf("confirmed outputs duplicated: %+v", dups)
	}
	if len(rejects) != 1 || rejects[0].Kind != RejectDuplicate {
		t.Fatalf("rejects=%+v, want 1 DUPLICATE", rejects)
	}
	t.Logf("最终聚合 a(count=%d,sum=%d) b(count=%d,sum=%d)，拒绝原因=%s 重复输出=%d",
		cntA, sumA, cntB, sumB, rejects[0].Kind, len(sink.Duplicates()))

	st := rt.Stats()
	if !st.Finished {
		t.Fatalf("runtime should finish cleanly: %+v", st)
	}
}

// waitFor 轮询条件直到成立或超时。
func waitFor(t *testing.T, cond func() bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}
