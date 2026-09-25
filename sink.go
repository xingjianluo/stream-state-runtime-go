package streamruntime

import "context"

// Sink 抽象下游输出。Deliver 必须在输出被下游持久/确认后才返回 nil。
// 返回错误或 ctx 取消时，运行时会认为该输出未确认，检查点后会重投。
type Sink interface {
	Deliver(ctx context.Context, rec OutputRecord) error
}

// RejectObserver 可选接收被拒绝事件的审计回调。
type RejectObserver func(re RejectedEvent)
