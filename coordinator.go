package streamruntime

import (
	"context"
	"errors"
	"time"
)

// 内部消息类型。
type ingested struct{ ev Event }
type shardEOF struct{ shard int }
type shardWatermark struct {
	shard int
	t     time.Time
}
type ackMsg struct {
	rec OutputRecord
	err error
}

// pendingOutput 是一条已生成输出的投递状态；存在于 map 中即表示尚未确认。
type pendingOutput struct {
	rec OutputRecord
}

// correction 是背压下暂缓的迟到修正重发请求。
type correction struct {
	win Window
	key string
}

// coordinator 承载全部可变状态，仅由 loop 所在 goroutine 访问。
type coordinator struct {
	r     *Runtime
	cfg   Config
	store CheckpointStore
	sink  Sink

	windows    *WindowState
	wm         *Watermark
	offsets    map[int]int64
	maxEventTs map[int]time.Time
	sourceWm   map[int]time.Time
	eof        map[int]bool

	nextSeq     int64
	pending     map[int64]pendingOutput // Seq -> 输出（待投递/投递中/待确认）
	corrections []correction            // 背压下暂缓的迟到修正
	inflight    map[int64]bool          // 当前已交给 deliverer、等待 ack 的 Seq
	delivered   int64
	rejDup      int64
	rejLate     int64
	cpCount     int64
	lastCPID    string
	finished    bool
}

func newCoordinator(r *Runtime) *coordinator {
	return &coordinator{
		r:          r,
		cfg:        r.cfg,
		store:      r.store,
		sink:       r.sink,
		windows:    NewWindowState(r.cfg.WindowSize),
		wm:         NewWatermark(),
		offsets:    make(map[int]int64),
		maxEventTs: make(map[int]time.Time),
		sourceWm:   make(map[int]time.Time),
		eof:        make(map[int]bool),
		pending:    make(map[int64]pendingOutput),
		inflight:   make(map[int64]bool),
	}
}

func (c *coordinator) logf(format string, args ...any) { c.r.logf(format, args...) }

// handleEvent 处理一条摄取进来的事件。
func (c *coordinator) handleEvent(e Event) {
	wm := c.wm.Value()
	kind, correctedKey := c.windows.Apply(e, wm, c.cfg.AllowedLateness)
	switch kind {
	case RejectDuplicate:
		c.rejDup++
		c.logf("拒绝重复事件 id=%s shard=%d offset=%d eventTime=%s wm=%s",
			e.ID, e.Shard, e.Offset, e.EventTime.Format("15:04:05.000"), wm.Format("15:04:05.000"))
		c.r.emitReject(RejectedEvent{Event: e, Kind: RejectDuplicate, Reason: "event id already aggregated", Watermark: wm})
		c.advanceOffset(e)
		return
	case RejectTooLate:
		c.rejLate++
		win := AssignWindow(e.EventTime, c.cfg.WindowSize)
		c.logf("拒绝迟到事件 id=%s shard=%d offset=%d eventTime=%s 窗口截止=%s wm=%s 允许延迟=%s",
			e.ID, e.Shard, e.Offset, e.EventTime.Format("15:04:05.000"),
			win.deadline(c.cfg.AllowedLateness).Format("15:04:05.000"),
			wm.Format("15:04:05.000"), c.cfg.AllowedLateness)
		c.r.emitReject(RejectedEvent{Event: e, Kind: RejectTooLate,
			Reason: "eventTime beyond window deadline (end + allowedLateness)", Watermark: wm})
		c.advanceOffset(e)
		return
	}

	// 接收：推进位点与分片已见最大事件时间。
	c.advanceOffset(e)
	if t, ok := c.maxEventTs[e.Shard]; !ok || e.EventTime.After(t) {
		c.maxEventTs[e.Shard] = e.EventTime
	}
	c.logf("接收事件 id=%s shard=%d offset=%d eventTime=%s key=%s value=%d",
		e.ID, e.Shard, e.Offset, e.EventTime.Format("15:04:05.000"), e.Key, e.Value)

	if correctedKey != "" {
		// 落入已触发窗口的迟到但合法事件：重发该 Key 的修正结果。
		if len(c.pending) >= c.cfg.MaxPendingOutputs {
			// 极端背压下暂不重发修正：聚合已在状态中更新，等 pending 有空位后，
			// 主循环不会再 flush 已触发 Key —— 因此这里需要在 handleEvent 外补发。
			c.logf("背压：迟到修正 key=%s 暂缓（pending=%d/%d）", correctedKey, len(c.pending), c.cfg.MaxPendingOutputs)
			c.corrections = append(c.corrections, correction{win: AssignWindow(e.EventTime, c.cfg.WindowSize), key: correctedKey})
		} else {
			win := AssignWindow(e.EventTime, c.cfg.WindowSize)
			if rec, ok := c.windows.EmitKey(win, correctedKey, wm); ok {
				c.logf("迟到但在允许延迟内 id=%s，修正窗口 %s..%s key=%s",
					e.ID, win.Start.Format("15:04:05"), win.End.Format("15:04:05"), correctedKey)
				c.admit([]OutputRecord{rec})
			}
		}
	}
}

func (c *coordinator) advanceOffset(e Event) {
	if off, ok := c.offsets[e.Shard]; !ok || e.Offset+1 > off {
		c.offsets[e.Shard] = e.Offset + 1
	}
}

// admit 为输出分配严格单调的 Seq 并放入 pending。调用方必须保证
// 加入后不会超过 MaxPendingOutputs（迟到修正每次最多 1 条，主循环据此放行）。
func (c *coordinator) admit(recs []OutputRecord) {
	if len(recs) == 0 {
		return
	}
	for i := range recs {
		c.nextSeq++
		recs[i].Seq = c.nextSeq
		c.pending[c.nextSeq] = pendingOutput{rec: recs[i]}
		c.logf("生成输出 seq=%d key=%s window=%s..%s sum=%d count=%d",
			c.nextSeq, recs[i].Key, recs[i].WindowStart.Format("15:04:05"),
			recs[i].WindowEnd.Format("15:04:05"), recs[i].Sum, recs[i].Count)
	}
	// 投递前检查点：位点/窗口状态（含 Key 触发标记）+ 待确认输出一起持久化，
	// 因此"已触发"与"可重投"原子绑定，崩溃不会丢输出也不会重复首次触发。
	c.checkpoint("emit")
}

// admitReadyWindows 先补发暂缓的迟到修正，再在 pending 空位内放行到点窗口。
func (c *coordinator) admitReadyWindows() {
	wm := c.wm.Value()
	// 1) 迟到修正优先补发（顺序确定：按暂缓顺序）。
	for len(c.corrections) > 0 && len(c.pending) < c.cfg.MaxPendingOutputs {
		cor := c.corrections[0]
		c.corrections = c.corrections[1:]
		if rec, ok := c.windows.EmitKey(cor.win, cor.key, wm); ok {
			c.admit([]OutputRecord{rec})
		}
	}
	// 2) 到点窗口的首次触发。
	room := c.cfg.MaxPendingOutputs - len(c.pending)
	if room > 0 {
		c.admit(c.windows.FlushLimited(wm, room))
	}
}

func (c *coordinator) handleEOF(shard int) {
	if !c.eof[shard] {
		c.eof[shard] = true
		c.logf("分片 %d EOF 已确认（%d/%d 分片结束）", shard, c.eofCount(), c.cfg.ShardCount)
	}
}

func (c *coordinator) eofCount() int {
	n := 0
	for i := 0; i < c.cfg.ShardCount; i++ {
		if c.eof[i] {
			n++
		}
	}
	return n
}

// handleSourceWatermark 记录分片源端水位线。
func (c *coordinator) handleSourceWatermark(shard int, t time.Time) {
	if cur, ok := c.sourceWm[shard]; ok && !t.After(cur) {
		return
	}
	c.sourceWm[shard] = t
}

// globalWatermark 计算全局水位线：min(各分片有效水位线)。
// 分片有效水位线 = max(已见最大事件时间, 源端上报水位线)。
// EOF 分片视为 +∞，不再拖低全局水位线。
// 任何非 EOF 分片尚无有效信息时，ok 返回 false（不推进）。
func (c *coordinator) globalWatermark() (time.Time, bool) {
	var global time.Time
	have := false
	for i := 0; i < c.cfg.ShardCount; i++ {
		if c.eof[i] {
			continue
		}
		var w time.Time
		ok := false
		if t, exists := c.maxEventTs[i]; exists {
			w, ok = t, true
		}
		if t, exists := c.sourceWm[i]; exists && (!ok || t.After(w)) {
			w, ok = t, true
		}
		if !ok {
			return time.Time{}, false
		}
		if !have || w.Before(global) {
			global, have = w, true
		}
	}
	if !have {
		// 所有分片都已 EOF：由 finishClean 处理全量触发，此处不推进。
		return time.Time{}, false
	}
	return global, true
}

// advanceWatermark 按周期性 tick 推进水位线，并触发到点窗口与过期清理。
func (c *coordinator) advanceWatermark() {
	candidate, ok := c.globalWatermark()
	if !ok {
		return
	}
	prev := c.wm.Value()
	cur, moved := c.wm.Advance(candidate)
	if !moved {
		return
	}
	c.logf("水位线推进 %s -> %s", prev.Format("15:04:05.000"), cur.Format("15:04:05.000"))

	// 必须在 purge 之前、同一协调步骤内原子地完成"触发 -> 分配Seq -> 入 pending"，
	// 否则窗口先被清理而输出尚未生成，会造成结果静默丢失。
	c.admitReadyWindows()

	// 清理超过允许延迟、且所有 Key 均已触发的窗口（Purge 自身保证不丢未生成输出）。
	purged := c.windows.Purge(cur, c.cfg.AllowedLateness)
	for _, w := range purged {
		c.logf("清理窗口 %s..%s（截止 %s 已被水位线越过）",
			w.Start.Format("15:04:05"), w.End.Format("15:04:05"),
			w.deadline(c.cfg.AllowedLateness).Format("15:04:05"))
	}
}

// handleAck 处理一次下游确认。
func (c *coordinator) handleAck(a ackMsg, deliver chan<- OutputRecord, retry *<-chan time.Time, makeTimer func() <-chan time.Time) {
	if !c.inflight[a.rec.Seq] {
		return
	}
	delete(c.inflight, a.rec.Seq)
	if a.err != nil {
		c.logf("下游确认失败 seq=%d err=%v，将重试（未确认结果不丢失）", a.rec.Seq, a.err)
		*retry = makeTimer()
		return
	}
	c.delivered = a.rec.Seq
	delete(c.pending, a.rec.Seq)
	c.logf("输出已确认 seq=%d key=%s，剩余 pending=%d", a.rec.Seq, a.rec.Key, len(c.pending))
	// 确认后立即检查点：位点/窗口状态不再需要为该输出保留重投责任。
	c.checkpoint("ack")
}

// nextToDeliver 返回当前可以投递的最小 Seq（严格有序，且无在投递项）。
func (c *coordinator) nextToDeliver() *OutputRecord {
	if len(c.inflight) > 0 {
		return nil
	}
	for seq := int64(1); seq <= c.nextSeq; seq++ {
		if p, ok := c.pending[seq]; ok {
			rc := p.rec
			return &rc
		}
	}
	return nil
}

// finishClean 在所有输入结束且输出排空后执行收尾：有界地全量触发残留窗口，
// 每轮只放行 pending 空位数量，待全部确认后写最终检查点并清空窗口。
func (c *coordinator) finishClean(ctx context.Context, deliver chan<- OutputRecord) error {
	if c.finished {
		return nil
	}
	wm := c.wm.Value()
	room := c.cfg.MaxPendingOutputs - len(c.pending)
	if room > 0 {
		// 先放行水位线已到点的窗口。
		c.admit(c.windows.FlushLimited(wm, room))
	}
	if room = c.cfg.MaxPendingOutputs - len(c.pending); room > 0 && c.windows.HasUnflushed() {
		// EOF 下水位线未越过的尾部窗口：无门槛、有界触发。
		c.admit(c.windows.FlushAllLimited(wm, room))
	}
	if c.windows.HasUnflushed() || len(c.pending) > 0 {
		return nil // 等待投递与确认后，由后续循环再次收尾
	}
	c.finished = true
	n := c.windows.PurgeAll()
	c.checkpoint("finish")
	c.logf("运行正常结束：全部窗口已触发确认，清理 %d 个窗口，最终检查点 id=%s", n, c.lastCPID)
	return errFinished
}

// errFinished 是协调循环正常结束的内部哨兵。
var errFinished = errors.New("streamruntime: finished")

func (c *coordinator) snapshot() Stats {
	return Stats{
		Watermark:         c.wm.Value(),
		ActiveWindows:     c.windows.ActiveWindowCount(),
		PendingOutputs:    len(c.pending),
		InFlightOutputs:   len(c.inflight),
		IngestBlocked:     c.r.ingestBlocked.Load(),
		DeliveredSeq:      c.delivered,
		RejectedDuplicate: c.rejDup,
		RejectedTooLate:   c.rejLate,
		Checkpoints:       c.cpCount,
		LastCheckpointID:  c.lastCPID,
		Finished:          c.finished,
	}
}
