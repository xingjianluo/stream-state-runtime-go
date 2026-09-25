package streamruntime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Stats 暴露运行时关键指标，供测试与审计观测。
type Stats struct {
	Watermark         time.Time
	ActiveWindows     int
	PendingOutputs    int
	InFlightOutputs   int
	IngestBlocked     int64
	DeliveredSeq      int64
	RejectedDuplicate int64
	RejectedTooLate   int64
	Checkpoints       int64
	LastCheckpointID  string
	Finished          bool
}

// Runtime 是可嵌入式的事件时间流处理运行时。
// 所有可变状态（窗口、位点、水位线、待确认输出、检查点）只由一个
// 协调 goroutine 访问，从根上消除并发状态错乱；摄取与输出通过有界
// 通道协作，慢下游会沿通道一路反压到分片读取。
type Runtime struct {
	cfg   Config
	store CheckpointStore
	sink  Sink
	logf  func(format string, args ...any)

	rejectMu sync.Mutex
	rejectFn RejectObserver

	statsMu      sync.Mutex
	finalStats   *Stats
	statsRunning bool

	// ingestBlocked 在摄取 goroutine 中累加（协调 goroutine 之外）。
	ingestBlocked atomic.Int64
	// statsReq 在 New 时建立，供运行中查询指标快照。
	statsReq chan chan Stats
	// runDone 在一次 Run 完全结束（含状态缓存）后关闭。
	runDone chan struct{}
}

// New 创建运行时。store 可为 nil（表示不做检查点持久化，恢复语义不适用）。
func New(cfg Config, store CheckpointStore, sink Sink) *Runtime {
	return &Runtime{
		cfg:      cfg,
		store:    store,
		sink:     sink,
		logf:     log.New(os.Stderr, "[streamruntime] ", log.LstdFlags|log.Lmicroseconds).Printf,
		statsReq: make(chan chan Stats),
		runDone:  make(chan struct{}),
	}
}

// SetLogger 替换日志函数，必须在 Run 之前调用。
func (r *Runtime) SetLogger(fn func(format string, args ...any)) {
	if fn != nil {
		r.logf = fn
	}
}

// SetRejectObserver 安装被拒绝事件的审计回调，必须在 Run 之前调用。
func (r *Runtime) SetRejectObserver(fn RejectObserver) {
	r.rejectMu.Lock()
	r.rejectFn = fn
	r.rejectMu.Unlock()
}

func (r *Runtime) emitReject(re RejectedEvent) {
	r.rejectMu.Lock()
	fn := r.rejectFn
	r.rejectMu.Unlock()
	if fn != nil {
		fn(re)
	}
}

// runIngester 为单个分片运行读取循环，把事件/EOF 送入 ingress 通道。
// ingress 满时发送阻塞，从而对上游读取形成确定的背压。
func (r *Runtime) runIngester(ctx context.Context, rd ShardReader, ingress chan<- any, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		e, err := rd.Read(ctx)
		if errors.Is(err, errEOF) {
			r.logf("分片 %d 读取结束 EOF", rd.ShardID())
			select {
			case ingress <- shardEOF{shard: rd.ShardID()}:
			case <-ctx.Done():
			}
			return
		}
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				r.logf("分片 %d 读取错误: %v", rd.ShardID(), err)
			}
			return
		}
		// 有界背压点：下游慢 -> pending 增长 -> ingress 停止排空 -> 此处阻塞。
		select {
		case ingress <- ingested{ev: e}:
		case <-ctx.Done():
			return
		}
	}
}

// runWatermarkPusher 周期性把分片源端水位线送入同一个有界 ingress 通道，
// 与事件保持严格的先后（同通道 FIFO）关系；背压时它与事件读取一起暂停。
func (r *Runtime) runWatermarkPusher(ctx context.Context, rd ShardReader, ingress chan<- any, wg *sync.WaitGroup, tick <-chan time.Time) {
	defer wg.Done()
	var last time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case _, ok := <-tick:
			if !ok {
				return
			}
		}
		if t, ok := rd.Watermark(); ok && t.After(last) {
			last = t
			select {
			case ingress <- shardWatermark{shard: rd.ShardID(), t: t}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// runDeliverer 串行确认输出，保证输出顺序确定；无缓冲通道天然背压。
func (r *Runtime) runDeliverer(ctx context.Context, deliver <-chan OutputRecord, acks chan<- ackMsg, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case rec, ok := <-deliver:
			if !ok {
				return
			}
			err := r.sink.Deliver(ctx, rec)
			select {
			case acks <- ackMsg{rec: rec, err: err}:
			case <-ctx.Done():
				return
			}
		}
	}
}

// Recover 仅加载并校验最新检查点（不启动运行时），供嵌入方在 Run 前审计恢复内容。
// 无检查点时返回 ErrNoCheckpoint；损坏/不兼容时返回对应明确错误。
func (r *Runtime) Recover() (*Checkpoint, error) {
	if err := r.cfg.Validate(); err != nil {
		return nil, err
	}
	if r.store == nil {
		return nil, fmt.Errorf("%w: no checkpoint store configured", ErrInvalidConfig)
	}
	cp, err := r.store.LoadLatest()
	if err != nil {
		return nil, err
	}
	r.logf("检查点可读 id=%s wm=%s offsets=%v pending=%d nextSeq=%d",
		cp.ID, cp.EventWatermark.Format(time.RFC3339Nano), cp.Offsets, len(cp.PendingOutputs), cp.NextOutputSeq)
	return cp, nil
}

// Run 启动摄取、聚合、输出与检查点循环，直到所有分片 EOF、输出全部确认后正常返回。
// ctx 取消时尽快停止并返回 ctx 的错误；窗口状态不做清理，可经检查点再次恢复。
func (r *Runtime) Run(ctx context.Context, readers []ShardReader) error {
	if err := r.cfg.Validate(); err != nil {
		return err
	}
	if len(readers) != r.cfg.ShardCount {
		return fmt.Errorf("%w: got %d readers, want %d", ErrInvalidConfig, len(readers), r.cfg.ShardCount)
	}
	if r.sink == nil {
		return fmt.Errorf("%w: nil sink", ErrInvalidConfig)
	}

	c := newCoordinator(r)

	r.statsMu.Lock()
	if r.statsRunning {
		r.statsMu.Unlock()
		return fmt.Errorf("%w: Run already active", ErrRuntimeStopped)
	}
	r.statsRunning = true
	r.finalStats = nil
	r.runDone = make(chan struct{})
	r.statsMu.Unlock()

	// 尝试恢复；无检查点则全部位点从零开始；损坏/不兼容检查点直接明确失败。
	var startOffsets map[int]int64
	if r.store != nil {
		if cp, err := r.store.LoadLatest(); err == nil {
			c.applyCheckpoint(cp)
			startOffsets = cp.Offsets
		} else if !errors.Is(err, ErrNoCheckpoint) {
			return err
		}
	}
	for _, rd := range readers {
		if off, ok := startOffsets[rd.ShardID()]; ok && off > 0 {
			if err := rd.SeekOffset(off); err != nil {
				return fmt.Errorf("seek shard %d: %w", rd.ShardID(), err)
			}
		}
	}

	ingress := make(chan any, r.cfg.IngestBuffer)
	deliver := make(chan OutputRecord) // 无缓冲：sink 多慢，协调器就等多久
	acks := make(chan ackMsg)

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	wmTicker := time.NewTicker(r.cfg.WatermarkTick)
	defer wmTicker.Stop()
	cpTicker := time.NewTicker(r.cfg.CheckpointInterval)
	defer cpTicker.Stop()
	// 推送器用独立 ticker：不能与协调器共享同一通道（同一 tick 只会被一方收到）。
	pushTicker := time.NewTicker(r.cfg.WatermarkTick)
	defer pushTicker.Stop()

	var ingWG, delWG sync.WaitGroup
	for _, rd := range readers {
		ingWG.Add(2)
		go r.runIngester(runCtx, rd, ingress, &ingWG)
		go r.runWatermarkPusher(runCtx, rd, ingress, &ingWG, pushTicker.C)
	}
	delWG.Add(1)
	go r.runDeliverer(runCtx, deliver, acks, &delWG)

	loopErr := c.loop(runCtx, ingress, deliver, acks, wmTicker.C, cpTicker.C)

	// 缓存最终指标快照，供 Run 返回后审计查询。
	final := c.snapshot()
	r.statsMu.Lock()
	r.finalStats = &final
	r.statsRunning = false
	done := r.runDone
	r.statsMu.Unlock()
	close(done)

	// 停止摄取与投递并等待 goroutine 退出；先停 ctx，让所有可能的发送者解除阻塞。
	cancel()
	ingWG.Wait()
	delWG.Wait()
	return loopErr
}

// Stats 返回运行时指标快照；运行结束后返回最终快照。
func (r *Runtime) Stats() Stats {
	r.statsMu.Lock()
	final := r.finalStats
	running := r.statsRunning
	done := r.runDone
	r.statsMu.Unlock()
	if !running {
		if final != nil {
			return *final
		}
		return Stats{IngestBlocked: r.ingestBlocked.Load()}
	}
	reply := make(chan Stats, 1)
	select {
	case r.statsReq <- reply:
		return <-reply
	case <-done:
		// 协调循环已退出：等待最终快照可见后返回，避免关闭竞态。
		r.statsMu.Lock()
		final = r.finalStats
		r.statsMu.Unlock()
		if final != nil {
			return *final
		}
		return Stats{IngestBlocked: r.ingestBlocked.Load()}
	}
}
