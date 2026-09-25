# stream-state-runtime-go

可嵌入式的事件时间流处理运行时。面向乱序、重复投递、进程重启与慢下游场景，
保证处理结果一致且可审计，全部行为可在本地复现与验证。

## 处理语义

### 事件时间窗口与水位线

- 事件按 `EventTime` 分配到左闭右开的滚动窗口 `[Start, End)`（`WindowSize` 对齐到 Unix 纪元）。
- 每个上游分片独立维护水位线，组合水位线取各分片最小值；**水位线只单调推进**，回退被忽略。
- 窗口 `End + AllowedLateness <= 水位线` 时触发，产出 `(窗口, 键)` 的 `Sum/Count` 聚合结果。
- 迟到判定：`EventTime <= 水位线 - AllowedLateness` 的事件被拒绝（`RejectLate`），不计入聚合，
  拒绝原因写入日志与 `Stats.LateDropped`。
- 重复投递：同一 `Event.ID` 只计入一次（`DedupStore`），重复事件计入 `Stats.Duplicates`。
- 结束语义：所有分片耗尽后，水位线推进到正无穷，触发全部剩余窗口，结果确定且可重放。

### 检查点与恢复

检查点（`Checkpoint`）涵盖四类状态，构成一致性边界：

| 状态 | 内容 |
|---|---|
| 消费位点 | 每个分片下一条待消费的 offset |
| 窗口状态 | 未触发窗口的聚合中间结果 |
| 去重集合 | 已处理事件 ID |
| 待确认输出 | 已触发但尚未成功写入 Sink 的输出 |

- 落盘格式：`JSON(数据) + SHA-256 校验和`，临时文件 + rename 原子提交，带格式版本号。
- 恢复流程：加载检查点 → 恢复位点/窗口/去重/水位线 → **先补发待确认输出** → 从位点继续消费。
- 一致性保证：
  - **已确认结果不重复**：Sink 按 `Output.ID`（窗口+键，确定性生成）幂等写入，
    恢复重放同一 ID 不产生重复结果。
  - **未确认结果不丢失**：崩溃时未确认的输出随检查点持久化，恢复后必然重发。
  - **损坏/不兼容明确拒绝**：校验和不匹配返回 `ErrCorruptCheckpoint`，
    版本不匹配返回 `ErrIncompatibleCheckpoint`，绝不静默继续。

### 背压与有界并发

- 每个分片一个摄取 goroutine，并行拉取；处理在单互斥锁下串行化，保证状态一致与结果确定。
- 在途事件（已拉取未处理）受 `InFlightLimit` 信号量约束：达到上限时摄取阻塞，
  内存占用有界（`InFlightLimit + Shards × FetchBatch` 条事件）。
- 下游变慢时，冲刷（flush）阻塞会自然传导为摄取阻塞；`ctx` 取消时所有阻塞点
  （拉取、信号量获取、Sink 写入）都可中断，无 goroutine 泄漏、无事件泄漏。

## 配置项

| 配置 | 默认 | 说明 |
|---|---|---|
| `WindowSize` | 1m | 滚动窗口大小 |
| `AllowedLateness` | 0 | 允许延迟，超出则拒绝迟到事件 |
| `FetchBatch` | 16 | 单次拉取最大事件数 |
| `InFlightLimit` | 64 | 在途事件上限（背压阈值） |
| `CheckpointEvery` | 32 | 每处理 N 条事件做一次检查点 |
| `CheckpointStore` | nil | 检查点存储（`FileCheckpointStore` 落盘） |
| `Logger` | nil | 审计日志回调：事件时间、水位线、检查点、背压判定依据 |

## 使用示例

```go
cfg := stream.Config{
    WindowSize:      10 * time.Second,
    AllowedLateness: 5 * time.Second,
    InFlightLimit:   64,
    CheckpointStore: &stream.FileCheckpointStore{Path: "state/cp.json"},
    Logger:          func(f string, a ...any) { log.Printf(f, a...) },
}
rt := stream.NewRuntime(cfg, source, sink)
if ok, err := rt.Recover(ctx); err != nil {
    // 损坏或不兼容检查点：明确失败，需人工介入
    return err
} else if ok {
    log.Println("已从检查点恢复")
}
if err := rt.Run(ctx); err != nil { /* 取消或上游错误，检查点已尽力保存 */ }
```

## 验证方法

```bash
go test ./stream/ -v          # 全部测试，日志含事件时间/水位线/检查点/背压依据
go test ./stream/ -race -count=3   # 竞态与确定性复验
```

测试覆盖：

- `TestOutOfOrderConsistency`：乱序流结果与参考聚合逐窗口一致
- `TestDuplicateEvents`：重复投递全部被去重，结果不变
- `TestLateEventDropped`：迟到事件拒绝且不计入聚合
- `TestDeterministicAcrossRuns`：并发下多次运行结果完全一致
- `TestCheckpointRecovery`：崩溃后恢复，已确认不重复、未确认不丢失
- `TestCheckpointCorruptAndIncompatible` / `TestRecoverFromCorruptCheckpoint`：损坏与版本不兼容检查点被明确拒绝
- `TestSlowSinkBackpressure`：慢下游下结果一致、内存有界
- `TestCancelNoLeak`：取消后无死锁、无 goroutine 泄漏
