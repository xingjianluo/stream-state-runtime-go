package stream

import (
	"context"
	"reflect"
	"sync"
	"time"
)

// Runtime 编排摄取、去重、窗口聚合、水位线、修正输出、背压与检查点。
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
	pending []Output       // 已产出但未确认（未冲刷成功）的输出：首次触发 + 修正
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
	Corrections int // 迟到数据触发的修正输出条数（Revision>1）
	DedupGC     int // 被安全回收的去重 ID 数
	Checkpoints int
}

func (r *Runtime) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stats
}

// StateSizes 返回当前内存中的窗口状态数与去重 ID 数（用于状态规模观测与测试）。
func (r *Runtime) StateSizes() (windows, dedup int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.wins.ActiveWindows(), r.dedup.Len()
}

// Recover 从检查点恢复状态；损坏或不兼容的检查点返回错误。
// 恢复后 pending 中的未确认输出（含修正）会在 Run 开始时重新冲刷，
// Sink 按 (Output.ID, Revision) 幂等，保证不重复、不回退。
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
	// 恢复后先补发未确认输出（含修正），保证不静默丢失；Sink 幂等保证不重复。
	if err := r.flushPending(ctx); err != nil {
		return err
	}

	n := r.source.Shards()
	// 派生内部 ctx：merge 一旦返回（正常耗尽、外部取消或拉取失败），立即取消，
	// 使仍阻塞在“取许可/发送事件”上的 prefetch goroutine 能走取消分支退出，
	// 避免“归并器已退出、预取仍等待接收方”的死锁。
	innerCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	chans := make([]chan Event, n)
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	for shard := 0; shard < n; shard++ {
		ch := make(chan Event) // 无缓冲：逐事件交付，每分片在归并器处至多积压 1 条
		chans[shard] = ch
		wg.Add(1)
		go r.prefetch(innerCtx, shard, ch, &wg, errCh)
	}

	merr := r.merge(innerCtx, chans, errCh)
	cancel()  // 通知所有 prefetch 停止阻塞并退出
	wg.Wait() // 确保所有预取 goroutine 退出，无泄漏
	close(errCh)
	if merr != nil {
		// 取消或失败：尽力保存检查点，保证已处理进度与已产出修正不丢失。
		_ = r.checkpoint(context.Background())
		return merr
	}
	for err := range errCh {
		if err != nil {
			_ = r.checkpoint(context.Background())
			return err
		}
	}

	// 结束语义：所有分片耗尽后，将水位线推进到正无穷，首次触发全部剩余窗口并清理。
	r.mu.Lock()
	finalWM := r.wm.AdvanceAll(time.Date(9999, 1, 1, 0, 0, 0, 0, time.UTC))
	outs := r.wins.FireReady(finalWM)
	r.pending = append(r.pending, outs...)
	gc := r.dedup.CollectBefore(finalWM, r.cfg.WindowSize, r.cfg.AllowedLateness)
	r.stats.DedupGC += gc
	r.cfg.logf("finalize: watermark=%s fired=%d dedupGC=%d pending=%d",
		finalWM, len(outs), gc, len(r.pending))
	r.mu.Unlock()

	if err := r.flushPending(ctx); err != nil {
		return err
	}
	return r.checkpoint(ctx)
}

// prefetch 为单个分片循环拉取批次，并逐事件交付给归并器。
// 每条事件“先取一个在途许可、再经无缓冲通道发送”：
//   - 绝不为尚未交付的事件持有第二个许可，因此不会出现
//     “占着部分许可等剩余许可、归并器却无事件可处理”的循环等待；
//   - 取到许可后若发送阻塞，事件要么被归并器收下处理（随后释放许可），
//     要么 ctx 取消，二者都不会造成许可泄漏或事件丢失。
//
// 每个分片在归并器处至多积压 1 条已交付事件（见 merge），有界上界
// 为 InFlightLimit + Shards×FetchBatch（Fetch 批次内其余事件仍在本 goroutine 栈上）。
func (r *Runtime) prefetch(ctx context.Context, shard int, ch chan<- Event, wg *sync.WaitGroup, errCh chan<- error) {
	defer wg.Done()
	r.mu.Lock()
	after := r.offsets[shard]
	r.mu.Unlock()
	for {
		batch, err := r.source.Fetch(ctx, shard, after, r.cfg.FetchBatch)
		if err != nil {
			select {
			case errCh <- err:
			case <-ctx.Done():
			}
			close(ch)
			return
		}
		if len(batch) == 0 {
			close(ch) // 分片耗尽
			return
		}
		for _, ev := range batch {
			if len(r.sem) == cap(r.sem) {
				r.cfg.logf("backpressure: shard=%d in-flight=%d limit=%d, 阻塞等待",
					shard, len(r.sem), cap(r.sem))
			}
			select {
			case r.sem <- struct{}{}:
			case <-ctx.Done():
				close(ch)
				return
			}
			select {
			case ch <- ev:
			case <-ctx.Done():
				<-r.sem // 事件未交付，释放刚取的许可。
				close(ch)
				return
			}
		}
		after += uint64(len(batch))
	}
}

// merge 是唯一的状态消费者：各分片事件经确定性轮转（round-robin）归并处理，
// 保证处理顺序只取决于输入（分片号与位点），与 goroutine 调度、到达先后无关，
// 从而水位线推进、首次触发与每次修正的 Revision 序列在多分片下完全确定。
// 每个分片在 pending 中至多积压 1 条已交付事件，保证确定性且内存有界。
func (r *Runtime) merge(ctx context.Context, chans []chan Event, errCh <-chan error) error {
	n := len(chans)
	pending := make([]*Event, n) // 各分片已交付但未处理的事件（至多 1 条）
	closed := make([]bool, n)    // prefetch 已关闭通道
	done := make([]bool, n)      // 通道已关闭且无积压事件
	active := n
	cursor := 0

	for active > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		// 任一分片拉取失败即快速终止（错误通道带缓冲，不阻塞预取 goroutine）。
		select {
		case err := <-errCh:
			if err != nil {
				return err
			}
		default:
		}

		// 非阻塞接收：仅对通道未关闭、且本地无积压事件的分片（每分片至多 1 条）。
		for s := 0; s < n; s++ {
			if closed[s] || pending[s] != nil {
				continue
			}
			select {
			case ev, ok := <-chans[s]:
				if !ok {
					closed[s] = true
				} else {
					ev := ev
					pending[s] = &ev
				}
			default:
			}
		}

		// 从上一轮位置起轮转选择下一个有积压事件的分片。
		chosen := -1
		for k := 0; k < n; k++ {
			s := (cursor + k) % n
			if !done[s] && pending[s] != nil {
				chosen = s
				break
			}
		}

		if chosen == -1 {
			// 无可立即处理的事件：把“通道已关闭且无积压”的分片正式标记结束。
			for s := 0; s < n; s++ {
				if !done[s] && closed[s] && pending[s] == nil {
					done[s] = true
					active--
				}
			}
			if active == 0 {
				return nil
			}
			// 等待仍未关闭的分片交付或取消。case 动态构造，显式记录 case -> shard 映射
			// （已关闭分片被排除，编号会错位，不能假设 case 编号等于 shard）。
			cases := make([]reflect.SelectCase, 0, active+1)
			caseShards := make([]int, 0, active)
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
			for s := 0; s < n; s++ {
				if !closed[s] {
					cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(chans[s])})
					caseShards = append(caseShards, s)
				}
			}
			chosenCase, val, recvOK := reflect.Select(cases)
			if chosenCase == 0 {
				return ctx.Err()
			}
			s := caseShards[chosenCase-1]
			if !recvOK {
				closed[s] = true
			} else {
				ev := val.Interface().(Event)
				pending[s] = &ev
			}
			continue
		}

		// 处理该事件并释放其在途许可。
		ev := *pending[chosen]
		r.process(ev)
		<-r.sem
		pending[chosen] = nil
		if closed[chosen] {
			done[chosen] = true
			active--
		}
		cursor = (chosen + 1) % n

		// 每条事件处理完即冲刷：首次触发与每次修正按确定顺序尽快送达下游。
		if err := r.flushPending(ctx); err != nil {
			return err
		}
	}
	return nil
}

// process 处理单条事件：去重 → 推进水位线 → 窗口聚合/修正 → 触发就绪窗口
// → 按安全边界回收去重 ID → 更新位点 → 定期检查点。
func (r *Runtime) process(ev Event) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.dedup.CheckAndMark(ev.ID, ev.EventTime) {
		r.stats.Duplicates++
		r.cfg.logf("reject: duplicate id=%s shard=%d offset=%d", ev.ID, ev.Shard, ev.Offset)
	} else {
		combined := r.wm.Advance(ev.Shard, ev.EventTime)
		if reason := r.wins.Add(ev, combined); reason == RejectLate {
			r.stats.LateDropped++
			r.cfg.logf("reject: late id=%s eventTime=%s watermark=%s windowClose=%s",
				ev.ID, ev.EventTime, combined,
				r.wins.Assign(ev.EventTime).End.Add(r.cfg.AllowedLateness))
		} else {
			r.stats.Processed++
		}
		outs := r.wins.FireReady(combined)
		for _, o := range outs {
			if o.Revision > 1 {
				r.stats.Corrections++
				r.cfg.logf("correct: id=%s revision=%d sum=%v count=%d watermark=%s",
					o.ID, o.Revision, o.Sum, o.Count, combined)
			}
		}
		if len(outs) > 0 {
			r.pending = append(r.pending, outs...)
		}
		// 水位线推进后回收已彻底关闭窗口的去重 ID（安全边界见 DedupStore.CollectBefore）。
		if gc := r.dedup.CollectBefore(combined, r.cfg.WindowSize, r.cfg.AllowedLateness); gc > 0 {
			r.stats.DedupGC += gc
			r.cfg.logf("dedup-gc: watermark=%s removed=%d remain=%d", combined, gc, r.dedup.Len())
		}
	}
	r.offsets[ev.Shard] = ev.Offset + 1

	total := r.stats.Processed + r.stats.Duplicates + r.stats.LateDropped
	if total > 0 && total%r.cfg.CheckpointEvery == 0 {
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
	r.cfg.logf("checkpoint: seq=%d offsets=%v watermark=%s windows=%d dedup=%d pending=%d",
		cp.Seq, cp.Offsets, time.Unix(0, cp.Watermark).UTC(), len(cp.Windows), len(cp.Dedup), len(cp.Pending))
	return nil
}
