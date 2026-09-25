package streamruntime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestTooLateRejection 验证超过允许延迟的事件被明确拒绝，拒绝原因可审计，且不计入聚合。
func TestTooLateRejection(t *testing.T) {
	cfg, err := NewConfig(1, time.Minute, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	cfg.WatermarkTick = 5 * time.Millisecond
	sink := NewTestSink()
	rt := New(cfg, nil, sink)
	rt.SetLogger(testLogger(t))

	var rejects []RejectedEvent
	rt.SetRejectObserver(func(re RejectedEvent) { rejects = append(rejects, re) })

	shard := NewMemShard(0)
	shard.AddEvent(Event{ID: "early", Offset: 0, Key: "a", EventTime: sec(5), Value: 1})
	shard.SetSourceWatermark(sec(60)) // 触发窗口 [00,60)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- rt.Run(ctx, []ShardReader{shard}) }()

	// 等窗口触发（wm=60，deadline=70）。
	waitFor(t, func() bool { return len(sink.Records()) >= 1 }, 2*time.Second)

	// 事件时间 00:50 在 deadline 01:10 前 -> 仍可接收（迟到但合法，产生修正 seq=2）。
	shard.AddEvent(Event{ID: "late-ok", Offset: 1, Key: "a", EventTime: sec(50), Value: 100})
	// 推进水位线越过 70 -> 之后 00:05 的事件永久太晚。
	shard.SetSourceWatermark(sec(71))
	waitFor(t, func() bool {
		return rt.Stats().Watermark.Equal(sec(71))
	}, 2*time.Second)
	shard.AddEvent(Event{ID: "too-late", Offset: 2, Key: "a", EventTime: sec(5), Value: 999})
	// 重复事件即便在窗口清理后也应可识别为重复——窗口已 purge，此处只会是 too-late。
	shard.Close()

	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}

	var sawTooLate, sawDup bool
	for _, re := range rejects {
		if re.Event.ID == "too-late" && re.Kind == RejectTooLate {
			sawTooLate = true
			if re.Watermark.IsZero() {
				t.Fatal("rejection must carry the watermark for audit")
			}
			t.Logf("迟到拒绝可审计: id=%s eventTime=%s wm=%s 原因=%q",
				re.Event.ID, re.Event.EventTime.Format("15:04:05"), re.Watermark.Format("15:04:05"), re.Reason)
		}
		if re.Kind == RejectDuplicate {
			sawDup = true
		}
	}
	if !sawTooLate {
		t.Fatalf("too-late event not rejected: %+v", rejects)
	}

	// 最终聚合（取最后一条 key=a 结果）必须不含 999。
	var final OutputRecord
	for _, r := range sink.Records() {
		if r.Key == "a" {
			final = r
		}
	}
	if final.Sum != 101 || final.Count != 2 {
		t.Fatalf("too-late value leaked into aggregate: sum=%d count=%d", final.Sum, final.Count)
	}
	t.Logf("最终聚合不含被拒值: sum=%d count=%d", final.Sum, final.Count)
	_ = sawDup
}

// TestIncompatibleCheckpoint 篡改 JSON 中的版本号，校验通过但语义版本不兼容，必须拒绝。
func TestIncompatibleCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cpPath := filepath.Join(dir, "cp.bin")
	cfg, _ := NewConfig(1, time.Minute, 0)

	// 先造一个合法检查点。
	cp := &Checkpoint{
		Version:        checkpointVersion,
		EventWatermark: sec(10),
		Offsets:        map[int]int64{0: 1},
		Windows:        nil,
		NextOutputSeq:  1,
	}
	if err := NewFileStore(cpPath).Save(cp); err != nil {
		t.Fatal(err)
	}

	// 读出原始帧，替换 JSON 中的版本字段（保持帧校验和同步重算不可行，
	// 因此这里直接构造版本错误但校验和正确的帧）。
	good := &Checkpoint{
		Version:        99, // 不兼容的载荷版本
		EventWatermark: sec(10),
		Offsets:        map[int]int64{0: 1},
		NextOutputSeq:  1,
	}
	// 直接走文件存储会带 version=1 的帧头校验通过，载荷校验由 readSnapshot.validate 拒绝。
	// 手动写帧：用内部 writeSnapshot 序列化（marshalPayload 会把 Version 保留）。
	if err := NewFileStore(cpPath).Save(good); err != nil {
		t.Fatal(err)
	}
	// good.Version 会被 marshalPayload 保留为 99（只在为 0 时才填默认值）。
	if _, err := New(cfg, NewFileStore(cpPath), NewTestSink()).Recover(); !errors.Is(err, ErrIncompatibleCheckpoint) {
		t.Fatalf("want ErrIncompatibleCheckpoint, got %v", err)
	} else {
		t.Logf("不兼容版本被拒绝: %v", err)
	}

	// 不存在的文件 -> ErrNoCheckpoint。
	if _, err := New(cfg, NewFileStore(filepath.Join(dir, "nonexistent")), NewTestSink()).Recover(); !errors.Is(err, ErrNoCheckpoint) {
		t.Fatalf("want ErrNoCheckpoint, got %v", err)
	}

	// 截断的文件 -> ErrCorruptCheckpoint。
	if err := os.WriteFile(cpPath, []byte("SRCP\n1\nabcd\n{bad"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := New(cfg, NewFileStore(cpPath), NewTestSink()).Recover(); !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("want ErrCorruptCheckpoint for truncated, got %v", err)
	}
}
