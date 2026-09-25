package streamruntime

import (
	"context"
	"sync"
	"time"
)

// MemShard 是一个内存版 ShardReader：事件按 Offset 升序存放，
// 支持恢复时按位点重定位，支持运行中动态推送事件并控制源端水位线。
type MemShard struct {
	mu     sync.Mutex
	id     int
	events []Event
	offset int64 // 下一条将返回事件的 Offset
	wm     time.Time
	wmSet  bool
	closed bool
	// waiters 是所有等待新事件/水位线/关闭的一次性信号通道；全部在 mu 内操作。
	waiters []chan struct{}
}

// NewMemShard 创建空分片。事件 Offset 必须严格升序。
func NewMemShard(id int) *MemShard {
	return &MemShard{id: id}
}

// signal 唤醒所有等待者；调用方无需持锁。
func (s *MemShard) signal() {
	s.mu.Lock()
	ws := s.waiters
	s.waiters = nil
	s.mu.Unlock()
	for _, ch := range ws {
		close(ch)
	}
}

// subscribe 登记一个等待通道，必须在持锁状态下调用。
func (s *MemShard) subscribeLocked() chan struct{} {
	ch := make(chan struct{})
	s.waiters = append(s.waiters, ch)
	return ch
}

// AddEvent 在尾部追加一条事件，并自动填充 Shard 字段。
// Offset 必须严格大于已有最大 Offset。
func (s *MemShard) AddEvent(e Event) {
	s.mu.Lock()
	if len(s.events) > 0 && e.Offset <= s.events[len(s.events)-1].Offset {
		s.mu.Unlock()
		panic("MemShard: offsets must be strictly increasing")
	}
	e.Shard = s.id
	s.events = append(s.events, e)
	s.mu.Unlock()
	s.signal()
}

// SetSourceWatermark 设置源端水位线并唤醒等待中的读取。
func (s *MemShard) SetSourceWatermark(t time.Time) {
	s.mu.Lock()
	s.wm = t
	s.wmSet = true
	s.mu.Unlock()
	s.signal()
}

// Close 标记分片永久结束，此后 Read 返回 io.EOF。
func (s *MemShard) Close() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.signal()
}

// ShardID 返回分片编号。
func (s *MemShard) ShardID() int { return s.id }

// Read 返回下一条 Offset >= 当前位点的事件；无新事件时阻塞直到事件到来、关闭或 ctx 取消。
func (s *MemShard) Read(ctx context.Context) (Event, error) {
	for {
		s.mu.Lock()
		for i := range s.events {
			if s.events[i].Offset >= s.offset {
				e := s.events[i]
				s.offset = e.Offset + 1
				s.mu.Unlock()
				return e, nil
			}
		}
		if s.closed {
			s.mu.Unlock()
			return Event{}, errEOF
		}
		ch := s.subscribeLocked()
		s.mu.Unlock()

		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-ch:
		}
	}
}

// Watermark 返回当前源端水位线。
func (s *MemShard) Watermark() (time.Time, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wm, s.wmSet
}

// SeekOffset 把读取位点重定位到 offset（下一条读取 Offset==offset 的事件）。
func (s *MemShard) SeekOffset(offset int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.offset = offset
	return nil
}

// BufferedCount 返回尚未被读取的事件数，测试辅助。
func (s *MemShard) BufferedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for i := range s.events {
		if s.events[i].Offset >= s.offset {
			n++
		}
	}
	return n
}
