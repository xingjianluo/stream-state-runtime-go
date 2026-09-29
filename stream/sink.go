package stream

import (
	"context"
	"sync"
)

// Sink 接收窗口输出（首次结果与迟到修正）。
//
// 下游幂等契约：
//   - 幂等键为 (Output.ID, Output.Revision)：同一修订版重复投递（重试/恢复重放）
//     最多生效一次。
//   - 同一 Output.ID 只保留最大 Revision：收到更新版本即覆盖旧结果，
//     旧版本重放不回退；任何时候都不得把多个版本累加。
type Sink interface {
	Write(ctx context.Context, out Output) error
}

// MemSink 是遵守修正幂等契约的内存 Sink，可注入延迟以模拟慢下游。
type MemSink struct {
	mu      sync.Mutex
	latest  map[string]Output   // 每个输出 ID 的最新版本
	history map[string][]int64  // 每个输出 ID 实际生效（被采用）过的 Revision 序列
	applied map[string][]Output // 每个输出 ID 实际生效过的完整输出序列（用于测试逐步收敛）
	order   []string            // 首次出现的输出 ID 顺序
	// Delay 可选：每次写入前调用以模拟阻塞（需自行处理 ctx 取消）。
	Delay func()
	// FailHook 可选：每次写入时调用，返回非 nil 即模拟下游瞬时失败（不改变本地状态），
	// 运行时会保留在 pending 并重试，用于验证“同一修订版重试只生效一次”。
	FailHook func(out Output) error
	// Writes 记录实际调用 Write 的次数（含幂等/陈旧拒绝与失败重试），用于审计。
	Writes int
}

func NewMemSink() *MemSink {
	return &MemSink{
		latest:  make(map[string]Output),
		history: make(map[string][]int64),
		applied: make(map[string][]Output),
	}
}

func (s *MemSink) Write(ctx context.Context, out Output) error {
	if s.Delay != nil {
		s.Delay()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.FailHook != nil {
		if err := s.FailHook(out); err != nil {
			return err // 瞬时失败：不改变本地状态，调用方保留 pending 并重试
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Writes++
	cur, exists := s.latest[out.ID]
	switch {
	case exists && out.Revision == cur.Revision:
		return nil // 同一版本重试：幂等，不重复生效
	case exists && out.Revision < cur.Revision:
		return nil // 旧版本重放：忽略，绝不回退
	default:
		// 新版本（含首次）：覆盖为最新结果
		if !exists {
			s.order = append(s.order, out.ID)
		}
		s.latest[out.ID] = out
		s.history[out.ID] = append(s.history[out.ID], out.Revision)
		s.applied[out.ID] = append(s.applied[out.ID], out)
		return nil
	}
}

// Results 按首次出现顺序返回每个输出 ID 的最新版本结果。
func (s *MemSink) Results() []Output {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Output, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.latest[id])
	}
	return out
}

// Revisions 返回某输出 ID 实际生效过的修订号序列（用于修正语义测试）。
func (s *MemSink) Revisions(id string) []int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int64(nil), s.history[id]...)
}

// Applied 返回某输出 ID 实际生效过的完整输出序列（用于逐步收敛断言）。
func (s *MemSink) Applied(id string) []Output {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Output(nil), s.applied[id]...)
}

// Get 返回某输出 ID 的最新结果与是否存在。
func (s *MemSink) Get(id string) (Output, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	o, ok := s.latest[id]
	return o, ok
}
