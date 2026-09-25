package streamruntime

import (
	"errors"
	"time"
)

// Event 是进入运行时的单条流事件。
// ID 用于全局去重；Shard/Offset 标识其在对应上游分片中的消费位点。
type Event struct {
	ID        string
	Shard     int
	Offset    int64
	Key       string
	EventTime time.Time
	Value     int64
}

// RejectKind 描述事件被拒绝的确定原因。
type RejectKind int

const (
	// RejectDuplicate 表示同一事件 ID 已被计入过聚合。
	RejectDuplicate RejectKind = iota + 1
	// RejectTooLate 表示事件迟到超过允许延迟，对应窗口已清理。
	RejectTooLate
)

func (k RejectKind) String() string {
	switch k {
	case RejectDuplicate:
		return "DUPLICATE"
	case RejectTooLate:
		return "TOO_LATE"
	default:
		return "UNKNOWN"
	}
}

// RejectedEvent 记录一次被拒绝的事件及其原因，供审计。
type RejectedEvent struct {
	Event     Event
	Kind      RejectKind
	Reason    string
	Watermark time.Time
}

// OutputRecord 是一次窗口聚合产生的输出，Seq 在单个运行时生命周期内严格单调。
type OutputRecord struct {
	Seq         int64
	Key         string
	WindowStart time.Time
	WindowEnd   time.Time
	Watermark   time.Time
	Sum         int64
	Count       int64
}

// Config 汇总运行时全部可调参数。零值不可直接使用，请通过 NewConfig 构造。
type Config struct {
	ShardCount         int
	WindowSize         time.Duration
	AllowedLateness    time.Duration
	MaxPendingOutputs  int
	IngestBuffer       int
	CheckpointInterval time.Duration
	WatermarkTick      time.Duration
}

var (
	// ErrInvalidConfig 表示配置不合法。
	ErrInvalidConfig = errors.New("streamruntime: invalid config")
	// ErrNoCheckpoint 表示存储中不存在可用检查点。
	ErrNoCheckpoint = errors.New("streamruntime: no checkpoint")
	// ErrCorruptCheckpoint 表示检查点损坏或校验失败。
	ErrCorruptCheckpoint = errors.New("streamruntime: corrupt checkpoint")
	// ErrIncompatibleCheckpoint 表示检查点版本不兼容。
	ErrIncompatibleCheckpoint = errors.New("streamruntime: incompatible checkpoint")
	// ErrRuntimeStopped 表示运行时已停止，不再接受新操作。
	ErrRuntimeStopped = errors.New("streamruntime: runtime stopped")
)
