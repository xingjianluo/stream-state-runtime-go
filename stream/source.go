package stream

import (
	"context"
	"fmt"
)

// Source 提供分片化的可重放事件源，位点为单调递增的偏移量。
type Source interface {
	Shards() int
	// Fetch 返回 shard 中位点 > after 的最多 max 条事件；到达末尾返回空切片。
	Fetch(ctx context.Context, shard int, after uint64, max int) ([]Event, error)
}

// SliceSource 是基于内存切片的 Source 实现，便于测试与本地复现。
// 每个分片内事件按 Offset 升序存放，Offset 即切片下标。
type SliceSource struct {
	Data [][]Event
}

func (s *SliceSource) Shards() int { return len(s.Data) }

func (s *SliceSource) Fetch(ctx context.Context, shard int, after uint64, max int) ([]Event, error) {
	if shard < 0 || shard >= len(s.Data) {
		return nil, fmt.Errorf("stream: 非法分片 %d", shard)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	events := s.Data[shard]
	start := int(after)
	if start >= len(events) {
		return nil, nil
	}
	end := start + max
	if end > len(events) {
		end = len(events)
	}
	out := make([]Event, end-start)
	copy(out, events[start:end])
	return out, nil
}
