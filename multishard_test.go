package streamruntime

import (
	"context"
	"testing"
	"time"
)

// TestMultiShardWatermarkMinAndParallel 验证：
//   - 全局水位线 = min(各分片有效水位线)，慢分片拖住快分片；
//   - 多分片并行摄取结果正确，结束语义确定（全部 EOF 才收尾）。
func TestMultiShardWatermarkMinAndParallel(t *testing.T) {
	cfg, err := NewConfig(2, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	cfg.WatermarkTick = 5 * time.Millisecond
	cfg.CheckpointInterval = 50 * time.Millisecond

	s0 := NewMemShard(0)
	s1 := NewMemShard(1)
	s0.AddEvent(Event{ID: "a0", Offset: 0, Key: "k", EventTime: sec(30), Value: 1})
	s1.AddEvent(Event{ID: "b0", Offset: 0, Key: "k", EventTime: sec(45), Value: 10})

	sink := NewTestSink()
	rt := New(cfg, nil, sink)
	rt.SetLogger(testLogger(t))

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx, []ShardReader{s0, s1}) }()

	// 快分片 s0 水位线到 90，慢分片 s1 尚无水位线：全局不应越过 s1 的数据时间 45。
	s0.SetSourceWatermark(sec(90))
	time.Sleep(100 * time.Millisecond)
	st := rt.Stats()
	// 有效水位线来自 max(事件时间, 源水位)；s1 有效=45 -> 全局=45，窗口 [00,60) 不触发。
	if st.Watermark.After(sec(45)) {
		t.Fatalf("global wm must be held by slow shard, got %s", st.Watermark.Format("15:04:05"))
	}
	if len(sink.Records()) != 0 {
		t.Fatalf("window must not fire while slow shard wm=45 < end=60, got %d outputs", len(sink.Records()))
	}
	t.Logf("慢分片约束生效: s0源水位=%s s1有效=%s -> 全局水位=%s（窗口 60s 未触发）",
		sec(90).Format("15:04:05"), sec(45).Format("15:04:05"), st.Watermark.Format("15:04:05"))

	// 慢分片推进到 60：窗口触发，sum=11（两分片并行聚合到同一 key）。
	s1.SetSourceWatermark(sec(60))
	waitFor(t, func() bool { return len(sink.Records()) >= 1 }, 2*time.Second)
	s0.Close()
	s1.Close()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
	recs := sink.Records()
	if len(recs) != 1 || recs[0].Sum != 11 || recs[0].Count != 2 {
		t.Fatalf("multi-shard aggregate wrong: %+v", recs)
	}
	t.Logf("多分片并行聚合 window=%s..%s sum=%d count=%d",
		recs[0].WindowStart.Format("15:04:05"), recs[0].WindowEnd.Format("15:04:05"),
		recs[0].Sum, recs[0].Count)
}
