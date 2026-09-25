package streamruntime

import (
	"context"
	"sync"
	"time"
)

// TestSink 是测试用下游：可注入投递延迟模拟慢下游，记录每次投递，
// 并按 Seq 检测重复，供一致性审计。
type TestSink struct {
	mu       sync.Mutex
	got      []OutputRecord
	seenSeq  map[int64]int
	dups     []OutputRecord
	delay    time.Duration
	blockCh  chan struct{} // 非 nil 时 Deliver 阻塞直到该通道关闭或 ctx 取消
	deliverN int64
}

// NewTestSink 创建空测试下游。
func NewTestSink() *TestSink {
	return &TestSink{seenSeq: make(map[int64]int)}
}

// SetDelay 设置每次投递的固定延迟。
func (s *TestSink) SetDelay(d time.Duration) {
	s.mu.Lock()
	s.delay = d
	s.mu.Unlock()
}

// Block 让后续 Deliver 阻塞，直到调用 Release。
func (s *TestSink) Block() {
	s.mu.Lock()
	if s.blockCh == nil {
		s.blockCh = make(chan struct{})
	}
	ch := s.blockCh
	s.mu.Unlock()
	_ = ch
}

// Release 解除阻塞。
func (s *TestSink) Release() {
	s.mu.Lock()
	ch := s.blockCh
	s.blockCh = nil
	s.mu.Unlock()
	if ch != nil {
		close(ch)
	}
}

// Deliver 实现 Sink。
func (s *TestSink) Deliver(ctx context.Context, rec OutputRecord) error {
	s.mu.Lock()
	delay := s.delay
	ch := s.blockCh
	s.mu.Unlock()
	if ch != nil {
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deliverN++
	s.seenSeq[rec.Seq]++
	if s.seenSeq[rec.Seq] > 1 {
		s.dups = append(s.dups, rec)
	}
	s.got = append(s.got, rec)
	return nil
}

// Records 返回已确认记录的快照（按确认顺序）。
func (s *TestSink) Records() []OutputRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OutputRecord, len(s.got))
	copy(out, s.got)
	return out
}

// Duplicates 返回被重复投递的记录（正常情况下应为空）。
func (s *TestSink) Duplicates() []OutputRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]OutputRecord, len(s.dups))
	copy(out, s.dups)
	return out
}
