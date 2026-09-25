package stream

import (
	"context"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestSlowSinkBackpressure(t *testing.T) {
	shards := makeStream(120)
	sink := NewMemSink()
	var writes atomic.Int32
	sink.Delay = func() { // 模拟慢下游：每次写入阻塞 2ms
		writes.Add(1)
		time.Sleep(2 * time.Millisecond)
	}
	cfg := Config{
		WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second,
		InFlightLimit: 2, FetchBatch: 2, CheckpointEvery: 10,
	}
	cfg.Logger = func(format string, args ...any) { t.Logf(format, args...) }
	rt := NewRuntime(cfg, &SliceSource{Data: shards}, sink)

	start := time.Now()
	if err := rt.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Logf("慢下游完成: 写入=%d 耗时=%s (背压以 InFlightLimit=%d 限制在途事件)",
		writes.Load(), time.Since(start), cfg.InFlightLimit)
	// 慢下游不得影响结果一致性
	assertConsistent(t, resultsMap(sink), reference(shards, cfg.WindowSize))
}

func TestCancelNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	shards := makeStream(10000) // 足够多的事件，保证取消时仍在运行
	sink := NewMemSink()
	sink.Delay = func() { time.Sleep(time.Millisecond) }
	cfg := Config{
		WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second,
		InFlightLimit: 4, FetchBatch: 4,
	}
	cfg.Logger = func(format string, args ...any) { t.Logf(format, args...) }
	rt := NewRuntime(cfg, &SliceSource{Data: shards}, sink)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("取消后 Run 应返回错误")
		}
		t.Logf("取消后 Run 返回: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatal("取消后 Run 未在限定时间内返回（可能存在死锁或无界阻塞）")
	}

	// 等待 goroutine 退出，验证无泄漏
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= before+1 {
			t.Logf("goroutine 无泄漏: before=%d after=%d", before, runtime.NumGoroutine())
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("疑似 goroutine 泄漏: before=%d after=%d", before, runtime.NumGoroutine())
}
