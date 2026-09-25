# stream-state-runtime-go

一个**可嵌入式**的事件时间（event-time）流处理运行时（Go 库，无外部服务依赖）。
面向乱序、重复投递、进程重启与慢下游场景，提供**结果一致、可审计、本地可复现**的
滚动窗口聚合与至少一次（at-least-once）输出。

- 单协调 goroutine 持有全部可变状态，杜绝并发状态错乱；
- 水位线单调只增；乱序按「允许延迟」接收或明确拒绝；
- 以事件 ID 去重，重复投递绝不重复计入聚合（去重集合随检查点持久化）；
- 检查点原子落盘，严格校验版本与校验和，损坏/不兼容一律明确拒绝；
- 有界并发与通道背压：慢下游沿 `sink → pending → ingress → 上游 Read` 一路传导，
  内存占用有确定上界，任务取消时干净退出、不丢未确认结果。

---

## 1. 快速开始

```go
cfg, err := streamruntime.NewConfig(
    2,                 // 上游分片数
    time.Minute,       // 窗口大小（滚动窗口）
    10*time.Second,    // 允许延迟 AllowedLateness
)
if err != nil { return err }

store := streamruntime.NewFileStore("/var/lib/myapp/checkpoint.bin") // 原子单文件
rt := streamruntime.New(cfg, store, sink)                           // sink 实现 Sink 接口

rt.SetRejectObserver(func(re streamruntime.RejectedEvent) {
    // 审计：记录被拒绝事件、原因（DUPLICATE / TOO_LATE）与当时水位线
})

if err := rt.Run(ctx, readers); err != nil { // readers 实现 ShardReader
    log.Fatal(err) // context.Canceled 表示被取消，状态仍可从检查点恢复
}
```

不希望持久化时给 `store` 传 `nil`（此时不具备重启恢复能力，其余语义不变）。

## 2. 核心概念与处理语义

### 2.1 事件与位点

```go
type Event struct {
    ID        string    // 全局唯一，用于去重（跨分片也要唯一）
    Shard     int       // 上游分片编号
    Offset    int64     // 在分片中严格递增的消费位点
    Key       string    // 聚合分组键
    EventTime time.Time // 事件时间（决定窗口归属，而非处理时间）
    Value     int64     // 参与求和
}
```

检查点中保存的位点为「下一条待消费 offset」，即 `已消费事件 Offset + 1`。
恢复时调用 `ShardReader.SeekOffset(n)`，从位点 `n` 继续读。

### 2.2 水位线（单调只增）

- 每个分片的有效水位线 = `max(该分片已见最大事件时间, 分片源端上报水位线)`；
- 全局水位线 = `min(所有未结束分片的有效水位线)`——**慢分片拖住快分片**；
- 分片 EOF 后视为 +∞，不再参与最小值；
- `Watermark.Advance` 只接受严格更晚的时间，回退与相等推进都被拒绝；
- 水位线由固定 tick 周期性推进（`WatermarkTick`），推进时刻与取值确定。

### 2.3 窗口、乱序与允许延迟

- 滚动窗口 `[windowStart, windowEnd)`，按 `EventTime.Truncate(WindowSize)` 归属；
- 窗口对每个 Key 单独维护：聚合值（Sum/Count）、**已触发标记**、事件 ID 去重集合；
- **首次触发**：水位线越过 `windowEnd` 时输出该窗口各 Key 的结果（`FlushLimited`，有序）；
- **迟到但接收**：`eventTime` 对应的窗口在
  `windowEnd + AllowedLateness` 之后尚未被水位线越过时，事件照常计入；
  若该 Key 已触发过首次输出，则**重发一条修正结果**（新的 Seq，下游按
  `(Key, WindowStart)` 取最新即可）；
- **迟到拒绝**：水位线已越过 `windowEnd + AllowedLateness`，事件被判
  `RejectTooLate`，不计入聚合，经 `RejectObserver` 给出可审计原因与当时水位线；
- **重复拒绝**：同一事件 ID 已在窗口去重集合中，判 `RejectDuplicate`，绝不重复计入。

输出顺序确定：同一生命周期内 Seq 严格单调递增；投递严格按 Seq 串行；
窗口结果按窗口起点、再按 Key 字典序生成。

### 2.4 结束语义

- 只有**全部**分片 EOF、摄取通道排空、且无任何待确认输出时，才正常结束；
- EOF 时水位线未越过的尾部窗口也会被有界地全量触发，不丢尾批；
- 正常结束写「最终检查点」（窗口清空、位点齐全），随后 Run 返回 `nil`；
- `ctx` 取消时尽快停止并返回 `context.Canceled`，**不清理状态**，可从最近检查点恢复。

## 3. 检查点：状态与位点的一致性边界

### 3.1 检查点内容（一个不可分割的一致快照）

| 组成 | 说明 |
| --- | --- |
| `Offsets` | 每个分片「下一条待消费」位点 |
| `EventWatermark` | 水位线时间 |
| `Windows` | 全部窗口：各 Key 聚合值、各 Key 已触发标记、**事件 ID 去重集合** |
| `PendingOutputs` | 已生成但尚未被下游确认的全部输出（含 Seq 与完整内容） |
| `NextOutputSeq` | 下一个待分配 Seq，保证恢复后 Seq 不回退、不撞号 |

### 3.2 关键不变量

1. **先持久化、后投递**：输出一生成（分配 Seq、进入 pending）立即写检查点，
   然后才交给下游。崩溃只可能发生在「投递前」或「确认前」：
   - 投递前崩溃 → 恢复后从位点重放事件，去重集合保证不重复计入，窗口重新触发；
   - 确认前崩溃 → 输出已在 `PendingOutputs` 中，恢复后**原样重投**，不静默丢失。
2. **确认即推进**：下游 ack 成功后从 pending 移除并立刻写检查点；
   已确认结果不在快照中，恢复后**绝不重复出现**。
3. **位点只前进**：恢复时统一 `SeekOffset`；事件处理与位点推进在同一协调步骤。
4. **去重跨恢复生效**：重复判定来自持久化的事件 ID 集合，而不是仅靠位点
   （见测试 `TestDedupAcrossRecovery`，人为把位点拨回也不会重复计入）。
5. **触发即可重投**：窗口 Key 的「已触发」标记与对应输出在同一检查点落盘，
   不存在「窗口删了但输出从没生成」的窗口（purge 只清理所有 Key 均已触发、
   且越过延迟截止的窗口）。

> 输出语义是**至少一次**：下游必须以 `(Key, WindowStart)`（以及 Seq）做幂等。
> 运行时保证不丢、不重复计入；下游确认语义决定端到端 exactly-once。

### 3.3 文件格式与严格校验

单文件帧格式（魔数 + 帧版本 + 载荷 SHA-256 + JSON 载荷）：

```
SRCP\n
1\n
<sha256-hex(json)>\n
<json>
```

- 写入：临时文件 → `fsync` → 原子 `rename`（+ 目录 fsync），崩溃不会产生半写文件；
- 读取依次校验：魔数、帧版本（不兼容 → `ErrIncompatibleCheckpoint`）、
  校验和（不一致 → `ErrCorruptCheckpoint`）、载荷 JSON、语义一致性
  （位点非负、窗口 End>Start、聚合 Count 合法、pending Seq 严格递增且小于 NextSeq 等）；
- 文件不存在 → `ErrNoCheckpoint`（全新启动）。任何损坏都**显式失败**，绝不静默重置。

## 4. 有界并发与背压

- 每个分片一个摄取 goroutine（并行摄取），外加每个分片一个源水位线推送 goroutine；
  一个串行投递 goroutine；一个协调 goroutine。
- 内存上界（确定值）：

  `≤ ShardCount × IngestBuffer 条在途事件（单个共享有界 ingress 通道）`
  `+ MaxPendingOutputs 条待确认输出 + 活跃窗口状态（事件 ID 集合）`

- 背压链路：**sink 变慢/阻塞** → 投递阻塞 → pending 增长到 `MaxPendingOutputs`
  → 协调器停止从 `ingress` 取事件（`Stats.IngestBlocked` 计数并打印判定日志）
  → 有界 ingress 填满 → 摄取 goroutine 阻塞在发送上 → **停止从上游 Read**。
- 窗口触发也受同一上界约束：一次水位线推进最多放行「pending 剩余空位」条输出
  （`FlushLimited`），未触发的 Key 留待空位释放；迟到修正在满队时进入有界暂缓队列，
  空位出现后优先补发。因此输出洪峰不会突破 pending 上界。
- 取消传播：所有 goroutine 同时监听 `ctx.Done()`，无缓冲/有界通道发送都带取消分支，
  Run 等待全部 worker 退出后返回，不会出现向已退出协程发送、事件泄漏或 panic。

## 5. 配置项

`NewConfig(shards, windowSize, allowedLateness)` 给出必填项与保守默认值；
可在调用 `New` 前覆盖其余字段（均为正整数时长/容量）：

| 字段 | 默认 | 含义 |
| --- | --- | --- |
| `ShardCount` | 必填（>0） | 上游分片数，必须与传入 reader 数量一致 |
| `WindowSize` | 必填（>0） | 滚动窗口大小 |
| `AllowedLateness` | 必填（≥0） | 窗口结束后仍接收迟到事件的宽限时间 |
| `MaxPendingOutputs` | 16 | 待确认输出上界，背压闸门 |
| `IngestBuffer` | 64 | 摄取通道容量（在途事件上界相关） |
| `CheckpointInterval` | 1s | 周期性检查点间隔（emit/ack 时还会额外写） |
| `WatermarkTick` | 20ms | 水位线推进/源水位线采样周期 |

`Config.Validate()` 在 `New`/`Run`/`Recover` 时强制校验，非法配置立即返回
`ErrInvalidConfig`。

## 6. 可观测与审计

- `Runtime.Stats()` 提供快照：当前水位线、活跃窗口数、pending/inflight 数、
  摄取阻塞计数、已确认最大 Seq、两类拒绝计数、检查点计数与最后检查点 ID、是否已结束；
- 默认日志输出到 stderr（`[streamruntime]` 前缀，带时间戳），可用 `SetLogger` 转接
  （例如测试中接入 `t.Logf`）。日志覆盖关键判定：
  事件接收/拒绝原因、水位线推进、窗口触发与修正、投递与确认、
  **每次检查点的位点/窗口/pending/nextSeq**、**背压判定依据**、窗口清理与结束；
- `SetRejectObserver` 接收 `RejectedEvent{Event, Kind, Reason, Watermark}`，
  拒绝原因可逐条核验。

## 7. 验证方法（本地复现）

全部行为为纯本地测试，无外部依赖：

```bash
go test ./...                 # 全部单测/端到端测试
go test -race ./...           # 带竞态检测
go test -race -count=3 ./...  # 多次运行消除时序偶发
go test -v -run TestE2EOutOfOrderAndDedup   # 乱序 + 重复
go test -v -run TestRecovery                # 检查点恢复（不丢/不重）
go test -v -run TestDedupAcrossRecovery     # 跨恢复去重
go test -v -run TestSlowDownstreamBoundedMemory # 慢下游背压有界
go test -v -run TestMultiShard              # 多分片水位线 min 与并行
go test -v -run TestIncompatibleCheckpoint  # 损坏/不兼容检查点拒绝
```

| 测试 | 覆盖场景 | 关键断言 |
| --- | --- | --- |
| `TestWatermarkMonotonic` | 水位线回退/相等 | 只允许严格前进 |
| `TestWindowAggregateOutOfOrderAndDedup` | 乱序、重复、迟到修正、purge | Sum/Count 正确且重复不计 |
| `TestE2EOutOfOrderAndDedup` | 端到端乱序+重复 | 聚合值、拒绝原因、无重复输出、干净结束 |
| `TestRecoveryNoLossNoDuplicate` | ack 前崩溃重启 | 未确认 seq=2 重投、已确认 seq=1 不重、位点正确恢复 |
| `TestDedupAcrossRecovery` | 恢复后重放同一事件 | 去重集合持久化，重放判 DUPLICATE，值不翻倍 |
| `TestRecoveryCorruptCheckpointRejected` | 翻转检查点字节 | 明确 `ErrCorruptCheckpoint` |
| `TestIncompatibleCheckpoint` | 版本号/截断/缺失 | 分别对应 incompatible / corrupt / no-checkpoint |
| `TestSlowDownstreamBoundedMemory` | sink 停滞 + 取消 | `IngestBlocked>0`、pending 不越界、取消干净退出 |
| `TestMultiShardWatermarkMinAndParallel` | 快慢分片 | 全局 wm 取 min、并行聚合 Sum 正确、全 EOF 才结束 |
| `TestTooLateRejection` | 超宽限迟到 | TOO_LATE 可审计且被拒值不入聚合 |

测试日志（`-v`）会打印关键事件时间、水位线推进、检查点内容与背压判定依据，
可直接用于审计与本地复现。

## 8. 主要源码结构

| 文件 | 职责 |
| --- | --- |
| `types.go` | Event/OutputRecord/Config/错误哨兵等公共类型 |
| `watermark.go` | 并发安全、单调只增的水位线 |
| `window.go` | 窗口聚合、per-Key 触发标记、去重、有界触发/清理、快照 |
| `checkpoint.go` | 检查点结构、帧格式、SHA-256、严格校验 |
| `store.go` | `CheckpointStore` 接口与原子单文件 `FileStore` |
| `coordinator.go` | 单线程状态机：事件、水位线、输出生命周期、恢复装载 |
| `loop.go` | 协调主循环 select、背压闸门、检查点写入 |
| `runtime.go` | 装配、worker goroutine、取消与等待、Stats |
| `shard.go` / `memshard.go` | 上游读取接口与可脚本化的内存实现（测试复现用） |
| `sink.go` / `testkit.go` | 下游接口与可控测试 sink |
