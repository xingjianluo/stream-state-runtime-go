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
	// acks 记录每个输出 ID 已被下游确认（成功写入 Sink）的最高版本，
	// 用于恢复时丢弃过期重放，保证确认过的结果不再出现、修正进度不倒退。
	acks  map[string]int
	cpSeq uint64
	stats Stats

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
		acks:    make(map[string]int),
		sem:     make(chan struct{}, c.InFlightLimit),
	}
}

// Stats 返回运行时统计（用于测试与审计）。
type Stats struct {
	Processed   int
	Duplicates  int
	LateDropped int
	Corrections int // 因迟到数据而向下游补发的修正结果次数
	Purged      int // 已到期清理的 (窗口,键) 状态数
	Checkpoints int
}

// StateSize 反映运行时仍在保留的内存状态规模，长期运行应只与水位线附近的
// 有界窗口/事件有关，不随已处理事件总量增长。
type StateSize struct {
	Windows int // 未到期清理的 (窗口,键) 聚合状态数
	Dedup   int // 仍需用于识别重复投递的事件 ID 数
	Pending int // 已产出未确认的输出数
	Acks    int // 已确认输出版本记录数
}

func (r *Runtime) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

// StateSize 返回当前保留状态的规模（用于长时运行的稳定性验证与监控）。
func (r *Runtime) StateSize() StateSize {
	r.mu.Lock()
	defer r.mu.Unlock()
	return StateSize{
		Windows: r.wins.Len(),
		Dedup:   r.dedup.Len(),
		Pending: len(r.pending),
		Acks:    len(r.acks),
	}
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
	r.acks = make(map[string]int, len(cp.Acks))
	for id, rev := range cp.Acks {
		r.acks[id] = rev
	}
	// 过滤掉恢复前已确认的重放输出（含已被更高版本取代的旧修正），
	// 保证确认过的结果不再出现、修正进度不倒退。
	r.pending = r.pending[:0]
	for _, out := range cp.Pending {
		if r.acks[out.ID] >= out.Revision {
			continue
		}
		r.pending = append(r.pending, out)
	}
	r.cpSeq = cp.Seq
	r.cfg.logf("recover: checkpoint seq=%d offsets=%v watermark=%s windows=%d dedup=%d pending=%d acks=%d",
		cp.Seq, cp.Offsets, time.Unix(0, cp.Watermark).UTC(),
		len(cp.Windows), len(cp.Dedup), len(r.pending), len(r.acks))
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

	// 结束语义：所有分片耗尽后，将水位线推进到正无穷，触发并清理全部剩余窗口。
	finalWM := time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC)
	r.mu.Lock()
	r.wm.AdvanceAll(finalWM)
	r.pending = append(r.pending, r.wins.EmitReady(finalWM)...)
	r.advanceAndGC(finalWM)
	r.cfg.logf("finalize: watermark=%s pending=%d", finalWM, len(r.pending))
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

// process 处理单条事件：去重 → 推进水位线 → 窗口聚合 → 触发/修正/清理 → 定期检查点。
func (r *Runtime) process(ev Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	w := r.wins.Assign(ev.EventTime)
	if !r.dedup.CheckAndMark(ev.ID, w.End.Add(r.cfg.AllowedLateness)) {
		r.stats.Duplicates++
		r.cfg.logf("reject: duplicate id=%s shard=%d offset=%d", ev.ID, ev.Shard, ev.Offset)
		// 重复事件不推进水位线，但其所属窗口可能早已过清理线；按当前水位线
		// 补做一次安全回收，避免流尾只来重复件时去重集合残留。
		if n := r.dedup.GCExpired(r.wm.Combined()); n > 0 {
			r.cfg.logf("dedup-gc: watermark=%s removed=%d retained=%d",
				r.wm.Combined(), n, r.dedup.Len())
		}
	} else {
		combined := r.wm.Advance(ev.Shard, ev.EventTime)
		reason, corrected := r.wins.Add(ev, combined)
		if reason == RejectLate {
			r.stats.LateDropped++
			r.cfg.logf("reject: late id=%s eventTime=%s watermark=%s allowedLateness=%s",
				ev.ID, ev.EventTime, combined, r.cfg.AllowedLateness)
		} else {
			r.stats.Processed++
			if corrected {
				// 事件落入已触发且仍在迟到容忍期内的窗口：补发新版本的修正结果。
				if out, ok := r.wins.EmitCorrection(w, ev.Key, combined); ok {
					r.stats.Corrections++
					r.cfg.logf("correct: id=%s revision=%d sum=%v count=%d",
						out.ID, out.Revision, out.Sum, out.Count)
					r.pending = append(r.pending, out)
				}
			}
		}
		// 首次触发水位线已到（End <= wm）、但迟到容忍期未结束的窗口。
		outs := r.wins.EmitReady(combined)
		if len(outs) > 0 {
			r.cfg.logf("fire: watermark=%s outputs=%d", combined, len(outs))
			r.pending = append(r.pending, outs...)
		}
		r.advanceAndGC(combined)
	}
	r.offsets[ev.Shard] = ev.Offset + 1

	if r.stats.Processed+r.stats.Duplicates+r.stats.LateDropped > 0 &&
		(r.stats.Processed+r.stats.Duplicates+r.stats.LateDropped)%r.cfg.CheckpointEvery == 0 {
		r.checkpointLocked(context.Background())
	}
}

// advanceAndGC 在水位线推进后执行安全清理，调用时必须持有 r.mu：
//   - 清理 End+允许延迟 <= 水位线 的窗口状态：这些窗口绝不可能再被改变；
//   - 清理所属窗口已过清理线的去重 ID：窗口结果已不可变，之后到达的重复件
//     只会被判定为不可接受的迟到，不再需要用于识别重复投递。
//
// 清理边界内的数据全部保留：容忍期内可能再改变结果的输入，以及仍需被
// 认出来是重复投递的事件 ID，都不会被回收。
func (r *Runtime) advanceAndGC(combined time.Time) {
	purged := r.wins.PurgeExpired(combined)
	if len(purged) > 0 {
		for _, st := range purged {
			id := outputID(st.Window, st.Key)
			// 该窗口不会再有任何新版本；仅在无在途输出时回收确认记录。
			if !r.pendingHas(id) {
				delete(r.acks, id)
			}
		}
		r.stats.Purged += len(purged)
		r.cfg.logf("purge: watermark=%s windows=%d", combined, len(purged))
	}
	if n := r.dedup.GCExpired(combined); n > 0 {
		r.cfg.logf("dedup-gc: watermark=%s removed=%d retained=%d", combined, n, r.dedup.Len())
	}
}

func (r *Runtime) pendingHas(id string) bool {
	for i := range r.pending {
		if r.pending[i].ID == id {
			return true
		}
	}
	return false
}

// flushPending 将待确认输出写入 Sink。
//
// 写入前过滤掉已被更高版本取代或已确认过的输出（恢复重放或同版本重试场景）：
// 下游因此绝不会收到旧版本，已确认结果也不会再出现一遍。成功写入的输出按
// (ID, Revision) 记录为已确认（Sink 本身亦按版本幂等）。
func (r *Runtime) flushPending(ctx context.Context) error {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	r.mu.Lock()
	pending := append([]Output(nil), r.pending...)
	r.mu.Unlock()

	for _, out := range pending {
		if r.ackedAtLeast(out.ID, out.Revision) {
			r.cfg.logf("skip: id=%s revision=%d 已确认或已被更高版本取代", out.ID, out.Revision)
			continue
		}
		if err := r.sink.Write(ctx, out); err != nil {
			return err
		}
		r.markAcked(out.ID, out.Revision)
	}

	// process 可能在写入期间追加了新输出，因此按确认账本过滤当前完整队列，
	// 不依赖快照长度；未被确认的新输出继续保留。
	r.mu.Lock()
	rem := r.pending[:0]
	dropped := make(map[string]struct{})
	for _, out := range r.pending {
		if r.acks[out.ID] >= out.Revision {
			dropped[out.ID] = struct{}{}
			continue
		}
		rem = append(rem, out)
	}
	r.pending = rem
	// 窗口已到期、且队列中不再有同 ID 在途输出时，回收确认记录。
	for id := range dropped {
		if !r.wins.HasOutputID(id) && !r.pendingHasAfter(r.pending, id) {
			delete(r.acks, id)
		}
	}
	r.mu.Unlock()
	return nil
}

// ackedAtLeast 判断 id 的 revision 版本是否已确认（或已被更高版本确认）。
func (r *Runtime) ackedAtLeast(id string, revision int) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.acks[id] >= revision
}

func (r *Runtime) markAcked(id string, revision int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if revision > r.acks[id] {
		r.acks[id] = revision
	}
}

// pendingHasAfter 判断 rem 中是否还存在 id 的输出（用于确认记录回收判定）。
func (r *Runtime) pendingHasAfter(rem []Output, id string) bool {
	for i := range rem {
		if rem[i].ID == id {
			return true
		}
	}
	return false
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
		Acks:      make(map[string]int, len(r.acks)),
	}
	for k, v := range r.offsets {
		cp.Offsets[k] = v
	}
	for k, v := range r.acks {
		cp.Acks[k] = v
	}
	if err := r.cfg.CheckpointStore.Save(ctx, cp); err != nil {
		return err
	}
	r.stats.Checkpoints++
	r.cfg.logf("checkpoint: seq=%d offsets=%v watermark=%s windows=%d dedup=%d pending=%d acks=%d",
		cp.Seq, cp.Offsets, time.Unix(0, cp.Watermark).UTC(),
		len(cp.Windows), r.dedup.Len(), len(cp.Pending), len(cp.Acks))
	return nil
}
