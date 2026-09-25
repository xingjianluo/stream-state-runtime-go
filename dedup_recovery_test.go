package streamruntime

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestDedupAcrossRecovery 验证去重集合随检查点持久化：
// 第一次运行消费 e1 并产生窗口输出（已进入检查点，未确认即"崩溃"）；
// 第二次运行恢复后，即便人为从更早位点重放 e1，该 ID 也会被去重集合拒绝，
// 且恢复的待确认输出原样重投（不丢、不重算成更大值）。
func TestDedupAcrossRecovery(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp.bin")
	cfg, _ := NewConfig(1, time.Minute, 5*time.Minute)
	cfg.WatermarkTick = 5 * time.Millisecond
	cfg.CheckpointInterval = time.Hour // 只依赖 emit/ack 检查点，避免干扰

	// 第一次：e1 触发窗口 seq=1，在 sink 确认前崩溃。
	shard := NewMemShard(0)
	shard.AddEvent(Event{ID: "e1", Offset: 0, Key: "a", EventTime: sec(10), Value: 7})
	shard.SetSourceWatermark(sec(60))

	sink1 := NewBlockingSink()
	hold1 := sink1.Hold(1)
	rt1 := New(cfg, NewFileStore(cpPath), sink1)
	rt1.SetLogger(testLogger(t))

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- rt1.Run(ctx1, []ShardReader{shard}) }()
	waitFor(t, func() bool {
		sink1.mu.Lock()
		n := sink1.seen[1]
		sink1.mu.Unlock()
		return n >= 1
	}, 3*time.Second)
	cancel1()
	_ = <-done1
	close(hold1)
	t.Log("第一次运行崩溃：seq=1 已生成并检查点，但未确认")

	// 第二次：恢复。人为把分片位点拨回 0 并重放同一 e1（offset=0）。
	shard2 := NewMemShard(0)
	shard2.AddEvent(Event{ID: "e1", Offset: 0, Key: "a", EventTime: sec(10), Value: 7})
	sink2 := NewTestSink()
	rt2 := New(cfg, NewFileStore(cpPath), sink2)
	rt2.SetLogger(testLogger(t))

	var rejects []RejectedEvent
	rt2.SetRejectObserver(func(re RejectedEvent) { rejects = append(rejects, re) })

	// 强制位点从 0 开始（覆盖恢复的 SeekOffset(1)），验证去重由窗口状态而非位点保证。
	shim := &forcedSeekShard{MemShard: shard2, forceOffset: 0}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	done2 := make(chan error, 1)
	go func() { done2 <- rt2.Run(ctx2, []ShardReader{shim}) }()

	// 恢复必须重投 seq=1，且值仍是 7（不是 14）。
	waitFor(t, func() bool { return len(sink2.Records()) >= 1 }, 3*time.Second)
	shard2.Close()
	if err := <-done2; err != nil {
		t.Fatalf("second run: %v", err)
	}

	recs := sink2.Records()
	if len(recs) != 1 {
		t.Fatalf("want exactly 1 redelivered output, got %+v", recs)
	}
	if recs[0].Sum != 7 || recs[0].Count != 1 {
		t.Fatalf("dedup failed across recovery: sum=%d count=%d (want 7/1)", recs[0].Sum, recs[0].Count)
	}
	foundDup := false
	for _, re := range rejects {
		if re.Event.ID == "e1" && re.Kind == RejectDuplicate {
			foundDup = true
		}
	}
	if !foundDup {
		t.Fatalf("replayed e1 must be rejected as duplicate, rejects=%+v", rejects)
	}
	t.Logf("跨恢复去重生效：重放 e1 被判 DUPLICATE，重投输出保持 sum=%d count=%d，无重复输出=%d",
		recs[0].Sum, recs[0].Count, len(sink2.Duplicates()))
}

// forcedSeekShard 包装 MemShard，忽略恢复的位点定位，强制从指定 offset 消费，
// 用于证明去重由持久化的事件 ID 集合保证，而非仅仅依赖位点。
type forcedSeekShard struct {
	*MemShard
	forceOffset int64
}

func (f *forcedSeekShard) SeekOffset(offset int64) error {
	return f.MemShard.SeekOffset(f.forceOffset)
}
