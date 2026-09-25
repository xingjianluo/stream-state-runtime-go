package stream

import (
	"context"
	"sync"
)

// Sink 接收窗口输出。写入必须按 Output.ID 幂等，重复提交同一 ID 不得产生重复结果。
type Sink interface {
	Write(ctx context.Context, out Output) error
}

// MemSink 是幂等的内存 Sink，可注入延迟以模拟慢下游。
type MemSink struct {
	mu    sync.Mutex
	seen  map[string]Output
	order []string
	// Delay 可选：每次写入前调用以模拟阻塞（需自行处理 ctx 取消）。
	Delay func()
	// Writes 记录实际调用 Write 的次数（含幂等拒绝的重复写入），用于审计。
	Writes int
}

func NewMemSink() *MemSink {
	return &MemSink{seen: make(map[string]Output)}
}

func (s *MemSink) Write(ctx context.Context, out Output) error {
	if s.Delay != nil {
		s.Delay()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Writes++
	if _, dup := s.seen[out.ID]; dup {
		return nil // 幂等：重复提交同一 ID 不产生重复结果
	}
	s.seen[out.ID] = out
	s.order = append(s.order, out.ID)
	return nil
}

// Results 按写入顺序返回去重后的全部输出。
func (s *MemSink) Results() []Output {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Output, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.seen[id])
	}
	return out
}
