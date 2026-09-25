package streamruntime

import (
	"context"
	"io"
	"time"
)

// errEOF 是分片永久结束时 Read 返回的哨兵错误（等同 io.EOF）。
var errEOF = io.EOF

// ShardReader 抽象一个上游分片的事件读取接口。
// Read 返回零值 Event 且 error 为 io.EOF 表示该分片已永久结束。
type ShardReader interface {
	ShardID() int
	Read(ctx context.Context) (Event, error)
	// Watermark 返回该分片当前的源端水位线；无可用信息时 ok 为 false。
	Watermark() (time.Time, bool)
	// SeekOffset 在恢复时把分片定位到指定位点（下一条读取 Offset==offset 的事件）。
	SeekOffset(offset int64) error
}
