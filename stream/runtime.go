package stream

import (
	"context"
	"sync"
	"time"
)

// Runtime 编排摄取、去重、窗口聚合、水位线、背压与检查点。
type Runtime struct {
	cfg    Config
	source Source
	sink   Sink

	wm    *WatermarkTracker
	dedup *DedupStore
	wins  *WindowManager

	mu      sync.Mutex
	flushMu sync.Mutex     // 串行化 flushPending，避免并发冲刷截断同一 pending
	offsets map[int]uint64 // 每个分片下一条待消费的位点
	pending []Output       // 已产出但未确认（未随检查点提交后冲刷成功）的输出
	cpSeq   uint64
	stats   Stats

	sem chan struct{} // 在途事件信号量，实现有界并发与背压
}

func NewRuntime(cfg Config, src Source, sink Sink) *Runtime {
	c := cfg.withDefaults()
	return &Runtime{
		cfg:     c,
		source:  src,
		sink:    sink,
		wm:      NewWatermarkTracker(src.Shards()),
		dedup:   NewDedupStore(),
		wins:    NewWindowManager(c.WindowSize, c.AllowedLateness),
		offsets: make(map[int]uint64),
		sem:     make(chan struct{}, c.InFlightLimit),
	}
}

// Stats 返回运行时统计（用于测试与审计）。
type Stats struct {
	Processed   int
	Duplicates  int
	LateDropped int
	Checkpoints int
}

func (r *Runtime) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

// Recover 从检查点恢复状态；损坏或不兼容的检查点返回错误。
// 恢复后 pending 中的未确认输出会在 Run 开始时重新冲刷（Sink 幂等保证不重复）。
func (r *Runtime) Recover(ctx context.Context) (bool, error) {
	if r.cfg.CheckpointStore == nil {
		return false, nil
	}
	cp, ok, err := r.cfg.CheckpointStore.Load(ctx)
	if err != nil || !ok {
		return ok, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.offsets = cp.Offsets
	if cp.Watermark != 0 {
		r.wm.Restore(time.Unix(0, cp.Watermark).UTC())
	}
	r.wins.Restore(cp.Windows)
	r.dedup.Restore(cp.Dedup)
	r.pending = append([]Output(nil), cp.Pending...)
	r.cpSeq = cp.Seq
	r.cfg.logf("recover: checkpoint seq=%d offsets=%v watermark=%s windows=%d dedup=%d pending=%d",
		cp.Seq, cp.Offsets, time.Unix(0, cp.Watermark).UTC(), len(cp.Windows), len(cp.Dedup), len(cp.Pending))
	return true, nil
}

// Run 运行至所有分片消费完毕或 ctx 取消；返回前冲刷并确认全部待确认输出。
func (r *Runtime) Run(ctx context.Context) error {
	// 恢复后先补发未确认输出，保证不静默丢失。
	if err := r.flushPending(ctx); err != nil {
		return err
	}

	var wg sync.WaitGroup
	errCh := make(chan error, r.source.Shards())
	for shard := 0; shard < r.source.Shards(); shard++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			errCh <- r.consumeShard(ctx, shard)
		}(shard)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			// 取消或失败：尽力保存检查点，保证已处理进度不丢失。
			_ = r.checkpoint(context.Background())
			return err
		}
	}

	// 结束语义：所有分片耗尽后，将水位线推进到正无穷，触发全部剩余窗口。
	r.mu.Lock()
	finalWM := r.wm.AdvanceAll(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
	outs := r.wins.FireReady(finalWM)
	r.pending = append(r.pending, outs...)
	r.cfg.logf("finalize: watermark=%s fired=%d pending=%d", finalWM, len(outs), len(r.pending))
	r.mu.Unlock()

	if err := r.flushPending(ctx); err != nil {
		return err
	}
	return r.checkpoint(ctx)
}

// consumeShard 循环拉取并处理单个分片，直到分片耗尽或 ctx 取消。
func (r *Runtime) consumeShard(ctx context.Context, shard int) error {
	for {
		r.mu.Lock()
		after := r.offsets[shard]
		r.mu.Unlock()

		batch, err := r.source.Fetch(ctx, shard, after, r.cfg.FetchBatch)
		if err != nil {
			return err
		}
		if len(batch) == 0 {
			return nil // 分片耗尽
		}
		for _, ev := range batch {
			// 背压：在途事件达到 InFlightLimit 时阻塞获取信号量。
			if len(r.sem) == cap(r.sem) {
				r.cfg.logf("backpressure: shard=%d in-flight=%d limit=%d, 阻塞等待",
					shard, len(r.sem), cap(r.sem))
			}
			select {
			case r.sem <- struct{}{}:
			case <-ctx.Done():
				return ctx.Err()
			}
			r.process(ev)
			<-r.sem
		}
		// 每批结束后冲刷已触发输出，确认成功的输出不再出现在后续检查点的 pending 中。
		if err := r.flushPending(ctx); err != nil {
			return err
		}
	}
}

// process 处理单条事件：去重 → 推进水位线 → 窗口聚合 → 触发就绪窗口 → 定期检查点。
func (r *Runtime) process(ev Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.dedup.CheckAndMark(ev.ID) {
		r.stats.Duplicates++
		r.cfg.logf("reject: duplicate id=%s shard=%d offset=%d", ev.ID, ev.Shard, ev.Offset)
	} else {
		combined := r.wm.Advance(ev.Shard, ev.EventTime)
		if reason := r.wins.Add(ev, combined); reason == RejectLate {
			r.stats.LateDropped++
			r.cfg.logf("reject: late id=%s eventTime=%s watermark=%s allowedLateness=%s",
				ev.ID, ev.EventTime, combined, r.cfg.AllowedLateness)
		} else {
			r.stats.Processed++
		}
		outs := r.wins.FireReady(combined)
		if len(outs) > 0 {
			r.cfg.logf("fire: watermark=%s outputs=%d", combined, len(outs))
			r.pending = append(r.pending, outs...)
		}
	}
	r.offsets[ev.Shard] = ev.Offset + 1

	if r.stats.Processed+r.stats.Duplicates+r.stats.LateDropped > 0 &&
		(r.stats.Processed+r.stats.Duplicates+r.stats.LateDropped)%r.cfg.CheckpointEvery == 0 {
		r.checkpointLocked(context.Background())
	}
}

// flushPending 将待确认输出写入 Sink，成功后清空（确认）。
func (r *Runtime) flushPending(ctx context.Context) error {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	r.mu.Lock()
	pending := append([]Output(nil), r.pending...)
	r.mu.Unlock()
	for _, out := range pending {
		if err := r.sink.Write(ctx, out); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.pending = r.pending[len(pending):]
	r.mu.Unlock()
	return nil
}

// checkpoint 保存当前一致状态（位点 + 窗口 + 去重 + 水位线 + 待确认输出）。
func (r *Runtime) checkpoint(ctx context.Context) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.checkpointLocked(ctx)
}

func (r *Runtime) checkpointLocked(ctx context.Context) error {
	if r.cfg.CheckpointStore == nil {
		return nil
	}
	r.cpSeq++
	var wmns int64
	if wm := r.wm.Combined(); !wm.IsZero() {
		wmns = wm.UnixNano()
	}
	cp := Checkpoint{
		Version:   FormatVersion,
		Seq:       r.cpSeq,
		Offsets:   make(map[int]uint64, len(r.offsets)),
		Watermark: wmns,
		Windows:   r.wins.Snapshot(),
		Dedup:     r.dedup.Snapshot(),
		Pending:   append([]Output(nil), r.pending...),
	}
	for k, v := range r.offsets {
		cp.Offsets[k] = v
	}
	if err := r.cfg.CheckpointStore.Save(ctx, cp); err != nil {
		return err
	}
	r.stats.Checkpoints++
	r.cfg.logf("checkpoint: seq=%d offsets=%v watermark=%s windows=%d pending=%d",
		cp.Seq, cp.Offsets, time.Unix(0, cp.Watermark).UTC(), len(cp.Windows), len(cp.Pending))
	return nil
}
