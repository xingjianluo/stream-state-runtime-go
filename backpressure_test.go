package streamruntime

import (
	"context"
	"testing"
	"time"
)

// stallSink 放行前 permit 条，之后全部阻塞，模拟慢/停滞下游。
type stallSink struct {
	got    chan OutputRecord
	permit int
}

func (s *stallSink) Deliver(ctx context.Context, rec OutputRecord) error {
	if s.permit > 0 {
		s.permit--
		select {
		case s.got <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
		return nil
	}
	// 下游停滞：一直阻塞到取消。
	<-ctx.Done()
	return ctx.Err()
}

// TestSlowDownstreamBoundedMemory 验证慢下游下：
//  1. 背压最终传导到上游：运行期间观测到 IngestBlocked > 0；
//  2. 内存有界：取消前活跃窗口/pending/通道占用均不超过配置上界；
//  3. 取消后 Run 干净返回，无 goroutine/通道泄漏导致的 panic。
func TestSlowDownstreamBoundedMemory(t *testing.T) {
	cfg, err := NewConfig(1, 10*time.Second, 0)
	if err != nil {
		t.Fatal(err)
	}
	// 极小有界参数，便于确定性地打满。
	cfg.MaxPendingOutputs = 2
	cfg.IngestBuffer = 3
	cfg.WatermarkTick = 5 * time.Millisecond
	cfg.CheckpointInterval = time.Hour // 本测试不依赖检查点，减少噪音

	shard := NewMemShard(0)
	// 预先灌入大量同窗口事件；水位线不推进，因此窗口不会触发输出。
	// pending 上界由"窗口触发数"决定 —— 为打满 pending，需要多个窗口：
	// 改为每 10s 一个窗口，并设置高源水位线一次性触发多个窗口。
	for i := int64(0); i < 50; i++ {
		shard.AddEvent(Event{
			ID:        "e" + itoa(i),
			Offset:    i,
			Key:       "k",
			EventTime: sec(i), // 0..49s -> 5 个 10s 窗口
			Value:     1,
		})
	}
	shard.SetSourceWatermark(sec(60)) // 触发 5 个窗口 -> 5 条输出，远超 pending 上界 2

	sink := &stallSink{got: make(chan OutputRecord, 16), permit: 1}
	rt := New(cfg, nil, sink)
	rt.SetLogger(testLogger(t))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx, []ShardReader{shard}) }()

	// 第一条被放行，随后下游停滞。等待背压传导：IngestBlocked 出现。
	deadline := time.Now().Add(3 * time.Second)
	var blocked int64
	for time.Now().Before(deadline) {
		st := rt.Stats()
		blocked = st.IngestBlocked
		if blocked > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if blocked == 0 {
		cancel()
		t.Fatal("expected backpressure to propagate (IngestBlocked > 0)")
	}
	t.Logf("背压判定依据: 下游停滞 -> pending 达上限 %d -> 暂停从 ingress 读取 -> 摄取阻塞计数=%d",
		cfg.MaxPendingOutputs, blocked)

	// 有界性校验：pending 绝不超过 MaxPendingOutputs；窗口数不超过窗口总量。
	st := rt.Stats()
	if st.PendingOutputs > cfg.MaxPendingOutputs {
		t.Fatalf("pending unbounded: %d > %d", st.PendingOutputs, cfg.MaxPendingOutputs)
	}
	if st.ActiveWindows > 5 {
		t.Fatalf("active windows unbounded: %d", st.ActiveWindows)
	}
	t.Logf("有界内存快照 pending=%d(上限%d) inflight=%d activeWindows=%d",
		st.PendingOutputs, cfg.MaxPendingOutputs, st.InFlightOutputs, st.ActiveWindows)

	// 任务取消必须干净退出，无事件泄漏/重复。
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Fatalf("want context.Canceled, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("runtime did not stop after cancel")
	}
}

func itoa(i int64) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
