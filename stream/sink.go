package stream

import (
	"context"
	"sync"
)

// Sink 接收窗口输出。
//
// 写入按 (Output.ID, Output.Revision) 遵循 latest-wins 幂等语义：
//   - 同一 ID 的首次触发结果与迟到修正结果 ID 相同、Revision 递增；
//   - 相同 (ID, Revision) 重复写入（重试/恢复重放）不得产生重复效果；
//   - 更高 Revision 必须取代旧值；更低 Revision 必须被忽略，不得覆盖新值。
type Sink interface {
	Write(ctx context.Context, out Output) error
}

// MemSink 是版本化幂等的内存 Sink，可注入延迟以模拟慢下游。
type MemSink struct {
	mu    sync.Mutex
	seen  map[string]Output
	order []string
	// Delay 可选：每次写入前调用以模拟阻塞（需自行处理 ctx 取消）。
	Delay func()
	// Writes 记录实际调用 Write 的次数（含幂等拒绝的重复写入），用于审计。
	Writes int
	// StaleRejects 记录因 Revision 低于已存版本而被拒绝的写入次数。
	StaleRejects int
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
	if cur, ok := s.seen[out.ID]; ok {
		switch {
		case out.Revision == cur.Revision:
			return nil // 同版本重发：幂等，不产生重复结果
		case out.Revision < cur.Revision:
			s.StaleRejects++ // 旧版本修正晚到：忽略，绝不回退
			return nil
		}
		// out.Revision > cur.Revision：新版本修正，覆盖旧值，ID 首次出现位置不变。
	} else {
		s.order = append(s.order, out.ID)
	}
	s.seen[out.ID] = out
	return nil
}

// Results 按 ID 首次出现顺序返回各键的最新版本输出。
func (s *MemSink) Results() []Output {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Output, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.seen[id])
	}
	return out
}

// Revision 返回某 ID 当前已保存的最高版本；不存在返回 0。
func (s *MemSink) Revision(id string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.seen[id].Revision
}
