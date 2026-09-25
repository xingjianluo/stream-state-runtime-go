package streamruntime

import (
	"context"
	"errors"
	"time"
)

// loop 是运行时唯一的状态 goroutine：事件处理、水位线、窗口、输出确认、
// 检查点、指标查询全部在此串行执行，因此内部状态无需加锁。
func (c *coordinator) loop(
	ctx context.Context,
	ingress <-chan any,
	deliver chan<- OutputRecord,
	acks <-chan ackMsg,
	wmTick <-chan time.Time,
	cpTick <-chan time.Time,
) error {
	var retryCh <-chan time.Time
	makeRetry := func() <-chan time.Time { return time.NewTimer(5 * time.Millisecond).C }

	allEOF := func() bool {
		for i := 0; i < c.cfg.ShardCount; i++ {
			if !c.eof[i] {
				return false
			}
		}
		return true
	}

	for {
		// 正常结束：全部分片 EOF、ingress 已排空、无待/在投递输出。
		if allEOF() && len(ingress) == 0 && len(c.pending) == 0 && len(c.inflight) == 0 {
			if err := c.finishClean(ctx, deliver); errors.Is(err, errFinished) {
				return nil
			} else if err != nil {
				return err
			}
		}

		// 把到点窗口未触发的 Key 在 pending 空位内放行（背压下自然停止触发）。
		c.admitReadyWindows()

		// 背压核心：pending 达到上限时停止从 ingress 取事件。
		// 有界 ingress 通道随之填满，摄取 goroutine 阻塞在发送上，
		// 进而停止从上游 Read —— 内存占用被严格限制在
		// IngestBuffer 条事件 + MaxPendingOutputs 条输出以内。
		var ingressCh <-chan any
		if len(c.pending) < c.cfg.MaxPendingOutputs {
			ingressCh = ingress
		} else if n := c.r.ingestBlocked.Add(1); n == 1 || n%100 == 0 {
			c.logf("背压判定: pending=%d 达到上限 %d，暂停摄取（ingress 容量=%d 暂缓修正=%d）",
				len(c.pending), c.cfg.MaxPendingOutputs, cap(ingress), len(c.corrections))
		}

		// 有序输出：无在投递项时才挑选最小 Seq；投递通道无缓冲，sink 阻塞即背压。
		// 用带 1 个缓冲的 outCh 承载"待发送值"，保证 select 发送分支是纯 channel 操作。
		// 有序输出：无在投递项时才挑选最小 Seq；投递通道无缓冲，sink 阻塞即背压。
		var sendCh chan<- OutputRecord
		var sendRec OutputRecord
		if retryCh == nil {
			if rec := c.nextToDeliver(); rec != nil {
				sendCh, sendRec = deliver, *rec
			}
		}

		select {
		case <-ctx.Done():
			c.logf("任务取消，停止主循环；状态保留，可从最近检查点恢复")
			return ctx.Err()

		case reply := <-c.r.statsReq:
			reply <- c.snapshot()

		case m := <-ingressCh:
			switch v := m.(type) {
			case ingested:
				c.handleEvent(v.ev)
			case shardEOF:
				c.handleEOF(v.shard)
			case shardWatermark:
				c.handleSourceWatermark(v.shard, v.t)
			}

		case sendCh <- sendRec:
			c.inflight[sendRec.Seq] = true
			c.logf("投递输出 seq=%d key=%s window=%s..%s sum=%d count=%d pending=%d",
				sendRec.Seq, sendRec.Key, sendRec.WindowStart.Format("15:04:05"),
				sendRec.WindowEnd.Format("15:04:05"), sendRec.Sum, sendRec.Count, len(c.pending))

		case a := <-acks:
			c.handleAck(a, deliver, &retryCh, makeRetry)

		case <-retryCh:
			retryCh = nil

		case <-wmTick:
			c.advanceWatermark()

		case <-cpTick:
			c.checkpoint("periodic")
		}
	}
}

// checkpoint 在协调 goroutine 内构造一致快照并原子持久化。
// 快照严格包含三类必需状态：消费位点、窗口状态（含去重集合）、待确认输出。
func (c *coordinator) checkpoint(reason string) {
	if c.store == nil {
		return
	}
	cp := &Checkpoint{
		Version:        checkpointVersion,
		CreatedAt:      time.Now().UTC(),
		EventWatermark: c.wm.Value(),
		Offsets:        cloneOffsets(c.offsets),
		Windows:        c.windows.Snapshot(),
		NextOutputSeq:  c.nextSeq + 1,
		PendingOutputs: make([]pendingOutputSnapshot, 0, len(c.pending)),
	}
	for seq := int64(1); seq <= c.nextSeq; seq++ {
		if p, ok := c.pending[seq]; ok {
			cp.PendingOutputs = append(cp.PendingOutputs, pendingOutputSnapshot{Record: p.rec})
		}
	}
	if err := c.store.Save(cp); err != nil {
		// 持久化失败不改变内存语义：输出仍会被投递；崩溃恢复窗口为最近一次成功检查点。
		c.logf("检查点保存失败 reason=%s err=%v", reason, err)
		return
	}
	c.cpCount++
	c.lastCPID = cp.ID
	c.logf("检查点已持久化 reason=%s id=%s wm=%s offsets=%v windows=%d pending=%d nextSeq=%d",
		reason, cp.ID, cp.EventWatermark.Format("15:04:05.000"), cp.Offsets,
		len(cp.Windows), len(cp.PendingOutputs), cp.NextOutputSeq)
}

// applyCheckpoint 把通过严格校验的检查点装载到运行时状态。
func (c *coordinator) applyCheckpoint(cp *Checkpoint) {
	c.wm.Load(cp.EventWatermark)
	c.offsets = cloneOffsets(cp.Offsets)
	c.windows.LoadSnapshot(cp.Windows)
	c.nextSeq = cp.NextOutputSeq - 1
	c.pending = make(map[int64]pendingOutput, len(cp.PendingOutputs))
	for _, p := range cp.PendingOutputs {
		// 恢复的待确认输出一律视为未确认：重新投递，保证"未确认结果不静默丢失"。
		c.pending[p.Record.Seq] = pendingOutput{rec: p.Record}
	}
	c.inflight = make(map[int64]bool)
	c.lastCPID = cp.ID
	c.logf("恢复状态完成 wm=%s pending重投=%d nextSeq=%d",
		cp.EventWatermark.Format("15:04:05.000"), len(c.pending), c.nextSeq)
}

func cloneOffsets(in map[int]int64) map[int]int64 {
	out := make(map[int]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
