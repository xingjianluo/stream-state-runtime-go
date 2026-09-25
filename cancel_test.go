package streamruntime

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// blockForeverSink 永远阻塞，模拟彻底停滞的下游。
type blockForeverSink struct{}

func (blockForeverSink) Deliver(ctx context.Context, rec OutputRecord) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestCancelNoLeak 验证：下游永久阻塞导致背压时，取消 ctx 后 Run 必须干净返回，
// 反复启动/取消也不累积 goroutine（无泄漏、无向已退出协程发送）。
func TestCancelNoLeak(t *testing.T) {
	cfg, _ := NewConfig(2, 10*time.Second, 0)
	cfg.MaxPendingOutputs = 2
	cfg.IngestBuffer = 2
	cfg.WatermarkTick = 5 * time.Millisecond
	cfg.CheckpointInterval = time.Hour

	runOnce := func() {
		s0 := NewMemShard(0)
		s1 := NewMemShard(1)
		for i := int64(0); i < 30; i++ {
			s0.AddEvent(Event{ID: "a" + itoa(i), Offset: i, Key: "k", EventTime: sec(i), Value: 1})
			s1.AddEvent(Event{ID: "b" + itoa(i), Offset: i, Key: "k", EventTime: sec(i), Value: 1})
		}
		s0.SetSourceWatermark(sec(60))
		s1.SetSourceWatermark(sec(60))

		rt := New(cfg, nil, blockForeverSink{})
		rt.SetLogger(func(string, ...any) {}) // 静默，专注泄漏判定
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- rt.Run(ctx, []ShardReader{s0, s1}) }()

		// 让背压确实发生后再取消。
		waitFor(t, func() bool { return rt.Stats().IngestBlocked > 0 }, 3*time.Second)
		cancel()
		select {
		case err := <-done:
			if err != context.Canceled {
				t.Fatalf("want context.Canceled, got %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("Run did not return after cancel (goroutine leak)")
		}
	}

	before := runtime.NumGoroutine()
	for i := 0; i < 5; i++ {
		runOnce()
	}
	// 给予被唤醒的 goroutine 退出时间，再强制 GC 调度。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if runtime.NumGoroutine() <= before+2 {
			t.Logf("取消后无 goroutine 泄漏: before=%d after=%d", before, runtime.NumGoroutine())
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("possible goroutine leak: before=%d after=%d", before, runtime.NumGoroutine())
}
