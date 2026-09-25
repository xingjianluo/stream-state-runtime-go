package streamruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// BlockingSink 允许测试精确控制每条输出在何时确认。
type BlockingSink struct {
	mu      sync.Mutex
	got     []OutputRecord
	seen    map[int64]int
	release map[int64]chan struct{}
	auto    bool
}

func NewBlockingSink() *BlockingSink {
	return &BlockingSink{seen: make(map[int64]int), release: make(map[int64]chan struct{}), auto: true}
}

// Hold 让 Deliver 在确认指定 Seq 前阻塞。
func (s *BlockingSink) Hold(seq int64) chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auto = false
	ch := make(chan struct{})
	s.release[seq] = ch
	return ch
}

func (s *BlockingSink) Deliver(ctx context.Context, rec OutputRecord) error {
	s.mu.Lock()
	s.seen[rec.Seq]++
	s.got = append(s.got, rec)
	ch, held := s.release[rec.Seq]
	s.mu.Unlock()
	if held {
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (s *BlockingSink) Records() []OutputRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OutputRecord, len(s.got))
	copy(out, s.got)
	return out
}

// TestRecoveryNoLossNoDuplicate 验证：
//  1. 输出 seq=1 已确认；seq=2 已生成但在确认前"进程崩溃"（ctx 取消）。
//  2. 新运行时从检查点恢复：已确认的 seq=1 不得再次投递；未确认的 seq=2 必须重投。
//  3. 位点恢复后，已计入检查点窗口状态的事件不会被重复聚合（去重集合生效）。
func TestRecoveryNoLossNoDuplicate(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp.bin")

	cfg, err := NewConfig(1, time.Minute, 0)
	if err != nil {
		t.Fatal(err)
	}
	cfg.WatermarkTick = 10 * time.Millisecond
	cfg.CheckpointInterval = 50 * time.Millisecond

	shard := NewMemShard(0)
	shard.AddEvent(Event{ID: "e1", Offset: 0, Key: "a", EventTime: sec(10), Value: 1})
	shard.AddEvent(Event{ID: "e2", Offset: 1, Key: "a", EventTime: sec(20), Value: 2})
	shard.SetSourceWatermark(sec(60)) // 触发窗口 -> seq=1
	// 第二批事件：产生 seq=2（迟到修正不可能，这里用新窗口）。
	shard.AddEvent(Event{ID: "e3", Offset: 2, Key: "a", EventTime: sec(70), Value: 4})
	shard.SetSourceWatermark(sec(120)) // 触发第二个窗口 -> seq=2

	sink := NewBlockingSink()
	holdSeq2 := sink.Hold(2)

	rt := New(cfg, NewFileStore(cpPath), sink)
	rt.SetLogger(testLogger(t))

	ctx1, cancel1 := context.WithCancel(context.Background())
	done1 := make(chan error, 1)
	go func() { done1 <- rt.Run(ctx1, []ShardReader{shard}) }()

	// 等到 seq=2 已投递给 sink 但尚未确认（被 hold 住），此时崩溃。
	waitFor(t, func() bool {
		sink.mu.Lock()
		n := sink.seen[2]
		sink.mu.Unlock()
		return n >= 1
	}, 3*time.Second)
	t.Logf("崩溃前 sink 已收到投递: %d 条（seq=2 未确认）", len(sink.Records()))
	cancel1()
	if err := <-done1; err == nil {
		t.Fatal("expect canceled run to return ctx error")
	}
	t.Logf("第一次运行已崩溃/取消，检查点文件保留于 %s", cpPath)

	// 第二次运行：恢复。
	sink2 := NewBlockingSink()
	rt2 := New(cfg, NewFileStore(cpPath), sink2)
	rt2.SetLogger(testLogger(t))

	shard2 := NewMemShard(0)
	shard2.AddEvent(Event{ID: "e1", Offset: 0, Key: "a", EventTime: sec(10), Value: 1})
	shard2.AddEvent(Event{ID: "e2", Offset: 1, Key: "a", EventTime: sec(20), Value: 2})
	shard2.AddEvent(Event{ID: "e3", Offset: 2, Key: "a", EventTime: sec(70), Value: 4})
	// 恢复位点应为 3：这些事件不会被重新消费（SeekOffset(3) 跳过全部）。
	// 不再重复设置源端水位线/EOF，稍后由测试显式推进。

	ctx2, cancel2 := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel2()
	done2 := make(chan error, 1)
	go func() { done2 <- rt2.Run(ctx2, []ShardReader{shard2}) }()

	// 恢复后必须重投未确认的 seq=2；seq=1 已确认绝不再投递。
	waitFor(t, func() bool {
		sink2.mu.Lock()
		n2 := sink2.seen[2]
		_, got1 := sink2.seen[1]
		sink2.mu.Unlock()
		return n2 >= 1 && !got1
	}, 3*time.Second)

	recs := sink2.Records()
	if len(recs) != 1 || recs[0].Seq != 2 {
		t.Fatalf("recovery must redeliver exactly seq=2, got %+v", recs)
	}
	if recs[0].Sum != 4 || recs[0].Count != 1 {
		t.Fatalf("redelivered record content mismatch: %+v", recs[0])
	}
	_ = holdSeq2 // 第一次的 hold 随 ctx1 取消已解除，仅用于制造未确认窗口
	t.Logf("恢复后重投 seq=%d window=%s..%s sum=%d count=%d，且已确认的 seq=1 未重复出现",
		recs[0].Seq, recs[0].WindowStart.Format("15:04:05"), recs[0].WindowEnd.Format("15:04:05"),
		recs[0].Sum, recs[0].Count)

	// 让恢复运行正常收尾。
	shard2.SetSourceWatermark(sec(120))
	shard2.Close()
	select {
	case err := <-done2:
		if err != nil {
			t.Fatalf("second run: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("second run did not finish")
	}
	if dups := len(sink2.Records()); dups != 1 {
		t.Fatalf("recovery sink records=%d, want exactly 1 (seq=2)", dups)
	}
	// 位点校验：shard2 应被 SeekOffset(3)，无事件被重读。
	if shard2.BufferedCount() != 0 {
		t.Fatalf("shard2 buffered=%d, want 0 (offsets must resume past consumed events)", shard2.BufferedCount())
	}
	st := rt2.Stats()
	if !st.Finished || st.DeliveredSeq != 2 {
		t.Fatalf("final stats: %+v", st)
	}
}

// TestRecoveryCorruptCheckpointRejected 验证损坏检查点必须被明确拒绝而非静默重置。
func TestRecoveryCorruptCheckpointRejected(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp.bin")

	cfg, _ := NewConfig(1, time.Minute, 0)
	rt := New(cfg, NewFileStore(cpPath), NewTestSink())
	if _, err := rt.Recover(); err != ErrNoCheckpoint {
		t.Fatalf("fresh store: want ErrNoCheckpoint, got %v", err)
	}

	// 写一个合法检查点，再破坏其载荷。
	shard := NewMemShard(0)
	shard.AddEvent(Event{ID: "e1", Offset: 0, Key: "a", EventTime: sec(10), Value: 1})
	sink := NewTestSink()
	rt1 := New(cfg, NewFileStore(cpPath), sink)
	rt1.SetLogger(testLogger(t))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { _ = rt1.Run(ctx, []ShardReader{shard}) }()
	waitFor(t, func() bool {
		st := rt1.Stats()
		return st.Checkpoints > 0
	}, 2*time.Second)
	shard.SetSourceWatermark(sec(60))
	shard.Close()
	time.Sleep(100 * time.Millisecond)
	raw, err := os.ReadFile(cpPath)
	if err != nil {
		t.Fatal(err)
	}
	raw[len(raw)-2] ^= 0xFF
	if err := os.WriteFile(cpPath, raw, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Recover(); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("corrupt checkpoint: want ErrCorruptCheckpoint, got %v", err)
	} else {
		t.Logf("损坏检查点被明确拒绝: %v", err)
	}
}
