package stream

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

var errCrash = errors.New("模拟崩溃")

// crashSource 在投递 limit 批后返回错误，模拟进程崩溃。
type crashSource struct {
	inner *SliceSource
	limit int32
	n     atomic.Int32
}

func (c *crashSource) Shards() int { return c.inner.Shards() }
func (c *crashSource) Fetch(ctx context.Context, shard int, after uint64, max int) ([]Event, error) {
	if c.n.Add(1) > c.limit {
		return nil, errCrash
	}
	return c.inner.Fetch(ctx, shard, after, max)
}

func TestCheckpointRecovery(t *testing.T) {
	shards := makeStream(60)
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	cfg := Config{
		WindowSize: 10 * time.Second, AllowedLateness: 10 * time.Second,
		FetchBatch: 4, CheckpointEvery: 5, CheckpointStore: store,
	}
	cfg.Logger = func(format string, args ...any) { t.Logf(format, args...) }
	sink := NewMemSink()

	// 第一次运行：第 6 批拉取后崩溃
	crash := &crashSource{inner: &SliceSource{Data: shards}, limit: 6}
	rt1 := NewRuntime(cfg, crash, sink)
	err := rt1.Run(t.Context())
	if !errors.Is(err, errCrash) {
		t.Fatalf("应返回崩溃错误, got %v", err)
	}
	st1 := rt1.Stats()
	t.Logf("崩溃时 stats: %+v, sink 已确认输出=%d", st1, len(sink.Results()))
	if st1.Checkpoints == 0 {
		t.Fatal("崩溃前应已产生检查点")
	}

	// 第二次运行：从检查点恢复并跑完
	rt2 := NewRuntime(cfg, &SliceSource{Data: shards}, sink)
	ok, err := rt2.Recover(t.Context())
	if err != nil || !ok {
		t.Fatalf("恢复失败: ok=%v err=%v", ok, err)
	}
	if err := rt2.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	st2 := rt2.Stats()
	t.Logf("恢复后 stats: %+v", st2)

	// 恢复后已处理事件不得重复计入
	if st2.Duplicates != 0 {
		t.Fatalf("恢复后不应出现重复事件, got %d", st2.Duplicates)
	}
	// 结果必须与无故障参考运行一致：已确认不重复、未确认不丢失
	assertConsistent(t, resultsMap(sink), reference(shards, cfg.WindowSize))
	t.Logf("恢复后最终结果与参考一致, 窗口数=%d, sink 写入调用=%d(含幂等重放)",
		len(sink.Results()), sink.Writes)
}

func TestRecoverFromCorruptCheckpoint(t *testing.T) {
	shards := makeStream(10)
	dir := t.TempDir()
	store := &FileCheckpointStore{Path: filepath.Join(dir, "cp.json")}
	cfg := Config{WindowSize: 10 * time.Second, CheckpointStore: store}
	cfg.Logger = func(format string, args ...any) { t.Logf(format, args...) }

	// 先产生一个检查点，再人为截断破坏
	rt1 := NewRuntime(cfg, &SliceSource{Data: shards}, NewMemSink())
	if err := rt1.Run(t.Context()); err != nil {
		t.Fatal(err)
	}
	p := store.Path
	data, _ := os.ReadFile(p)
	if err := os.WriteFile(p, data[:len(data)/2], 0o644); err != nil {
		t.Fatal(err)
	}

	rt2 := NewRuntime(cfg, &SliceSource{Data: shards}, NewMemSink())
	_, err := rt2.Recover(t.Context())
	if !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("损坏检查点必须被明确拒绝, got %v", err)
	}
	t.Logf("损坏检查点拒绝原因: %v", err)
}
