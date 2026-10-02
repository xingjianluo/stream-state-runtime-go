# stream-state-runtime-go

可嵌入式的事件时间流处理运行时。面向乱序、重复投递、进程重启、慢下游与**长期在线**场景，
保证处理结果一致、迟到数据可修正、长期运行状态有界且可审计，全部行为可在本地复现与验证。

## 处理语义

### 事件时间窗口与水位线

- 事件按 `EventTime` 分配到左闭右开的滚动窗口 `[Start, End)`（`WindowSize` 对齐到 Unix 纪元）。
- 每个上游分片独立维护水位线，组合水位线取各分片最小值；**水位线只单调推进**，回退被忽略。
- 窗口 `End <= 水位线` 时**首次触发**，产出 `(窗口, 键)` 的 `Sum/Count` 聚合结果（`Revision=1`），
  但窗口状态**保留**，以接收允许延迟内的迟到数据。
- 重复投递：同一 `Event.ID` 只计入一次（`DedupStore`），重复事件计入 `Stats.Duplicates`。
- 结束语义：所有分片耗尽后，水位线推进到正无穷，触发全部剩余窗口，结果确定且可重放。

### 迟到数据与结果修正（Correction）

系统里有**两条相互独立的边界**，分别回答"这条输入收不收"和"这份状态能不能回收"，
配置语义以前者为准，允许延迟就是 `AllowedLateness` 本身，**不会被窗口大小放大**：

| 边界 | 判定式 | 作用 |
|---|---|---|
| **接收边界（按事件时间，逐事件）** | `eventTime + AllowedLateness > 水位线` 才接收 | 决定一条迟到事件是并入并产生修正，还是按 `RejectLate` 直接丢弃 |
| **回收边界（按窗口，逐窗口）** | `windowEnd + AllowedLateness <= 水位线` 才关闭 | 决定何时删除窗口聚合状态、回收相关去重 ID |

两条线相差最多一个窗口大小（窗口内最早事件时间 = `windowEnd - WindowSize`），
因此窗口在接收边界之后、回收边界之前会处于一个明确的中间阶段：

- **什么情况下迟到数据还能产生修正**：窗口已首次触发（`windowEnd <= 水位线`），
  但该事件满足 `eventTime + AllowedLateness > 水位线`——即它落后水位线的时间
  **不超过** `AllowedLateness`。此时事件并入同一 `(窗口, 键)` 并产出一条
  `Revision` 递增的修正。
- **什么情况下即使窗口还没彻底关闭也必须拒绝**：只要
  `eventTime + AllowedLateness <= 水位线`，该事件就**立即按迟到丢弃**，
  不计入聚合、不产生任何结果（包括不给下游补发修正）——**哪怕其所属窗口此刻
  尚未到达回收线、状态仍在内存里**。例如 10s 窗口、5s 允许延迟，水位线 17 时
  事件时间 11 的事件（11+5=16 ≤ 17）必须拒绝，而它所在窗口 `[10,20)` 的回收线
  是 25，此时窗口确实还开着。判定相等（恰好落后一个允许延迟）也按拒绝处理。
- 窗口到 `windowEnd + AllowedLateness <= 水位线` 时才**彻底关闭**并清理状态；
  此后该窗口的任何事件自然也都越过接收边界，一律拒绝。

下游契约（`Output` 字段）：

| 字段 | 语义 |
|---|---|
| `ID` | 确定性输出 ID，等于 `窗口起-窗口止/键`；同一 `(窗口, 键)` 多次结果的 ID 始终不变 |
| `Revision` | 单调修订号，从 `1` 开始：首次触发为 `1`，每并入一次迟到数据 `+1` |
| `Sum/Count` | **截至该修订版的完整累计值**（不是增量），下游直接整份覆盖采用，绝不累加多个版本 |

下游应这样使用：

- **按 `(ID, Revision)` 幂等**：相同修订版因重试或恢复重发，最多生效一次。
- **同一 `ID` 只采用最大 `Revision`**：收到更新版本即覆盖旧结果；旧版本晚到（乱序/重放）
  必须忽略，**不得回退**，也不得把不同版本相加。
- 因此“同一批数据无论以什么顺序到达、中途修正过几次、甚至重试重发”，每个 `(窗口, 键)`
  最终都只剩最新一版，与“所有数据一次性算完”的结果**逐窗口逐键完全一致**。

**多分片下的确定性**：摄取虽然并发（每分片一个 goroutine），但所有状态写入由唯一消费者按
**严格分片轮转（round-robin，按分片号与位点）**串行归并：归并器每轮只等待并消费当前游标
分片的下一条事件，其他分片提前就绪也不抢跑。处理顺序恒为
`(shard0,off0),(shard1,off0),…,(shardN,off0),(shard0,off1),…`，只取决于输入，与 goroutine
调度、各分片 Fetch 快慢、`FetchBatch` / `InFlightLimit` 取值无关。因此每次修正的
`Revision` 及其先后顺序在多分片并发、不同批次大小与不同调度下也是确定的。
（`InFlightLimit` 会被自动钳制到不小于分片数，保证严格轮转下各分片都能推进、不会循环等待。）

> 内置 `MemSink` 已实现上述“同版去重、旧版不回退、整版覆盖”的幂等语义，可直接作为参考实现。

### 状态清理的安全边界（长期在线、规模有界）

长期运行下，两类状态都在水位线推进时持续回收，规模稳定在与
"活跃窗口跨度 × 键数"相关的**常量级**，不随运行天数 / 总事件量增长。
**修复接收边界不会放松回收边界**：接收判定按事件时间收紧，回收仍只在窗口
回收线（`windowEnd + AllowedLateness <= 水位线`）进行，回收时机不后移：

- **窗口状态**：窗口在回收线 `End + AllowedLateness <= 水位线`（彻底关闭）时删除。
- **去重集合**：某事件 ID 所属窗口到达回收线后，才回收该 ID（`DedupStore.CollectBefore`）。

安全性论证（接收边界早于回收边界，回收只晚不早）：

1. 窗口内最晚的事件时间严格小于窗口 `End`，所以只要窗口尚未到达回收线，
   就**仍可能存在**满足接收边界的迟到事件（时间落在 `End` 附近的尾部）。
   这些可能再改变结果的窗口聚合状态与相关事件 ID **都不会被提前清理**。
2. 窗口到达回收线后，其任何事件都必然满足 `eventTime + AllowedLateness <= 水位线`，
   一律被接收边界拒绝、不计入聚合；此时即使去重 ID 已被回收，重复投递的副本
   也只会被当作迟到拒绝，**不会被重复计入**。

也就是说：**可能再改变结果的输入**和**之后仍需被认出的重复事件**，在窗口回收线前都受保护；
回收线之后回收它们不可能影响任何结果。可通过 `Runtime.StateSizes()` 观察
`(窗口状态数, 去重 ID 数)`，回收量计入 `Stats.DedupGC`。

> 注：状态驻留峰值与“最慢分片相对落后的时间跨度”有关。各分片事件时间大致齐步推进时
> （稳态在线服务的常态），峰值为常量；单分片极端领先只会让峰值短暂变大，最终都会回收，
> 不影响有界性与正确性。

### 检查点与恢复（格式 v2）

检查点（`Checkpoint`）涵盖五类状态，构成一致性边界：

| 状态 | 内容 |
|---|---|
| 消费位点 | 每个分片下一条待消费的 offset |
| 窗口状态 | 未彻底关闭窗口的聚合中间结果，**含 `Fired/Revision` 修正进度** |
| 去重集合 | 已处理事件 ID **及其事件时间**（用于恢复后继续安全回收） |
| 水位线 | 组合水位线 |
| 待确认输出 | 已触发但尚未成功写入 Sink 的输出（含首次结果与历次修正的 `Revision`） |

- 落盘格式：`JSON(数据) + SHA-256 校验和`，临时文件 + rename 原子提交，带格式版本号。
- **兼容范围：仅接受恰好等于当前格式版本（v2）的检查点。**
  - 旧版本（如 v1：窗口触发即清理、去重无事件时间、无修正字段）：返回
    `ErrIncompatibleCheckpoint`，错误信息说明“来自不兼容的旧版本”，**不会被当成新状态继续**。
  - 更新的版本（更高版本号）：同样明确拒绝，提示升级二进制，避免旧代码误读新状态。
  - 校验和不匹配 / 无法解析：返回 `ErrCorruptCheckpoint`。
- 恢复流程：加载检查点（版本+校验和校验）→ 恢复位点/窗口修正进度/去重/水位线
  → **先补发待确认输出（含修正，Sink 按 `(ID,Revision)` 幂等）** → 从位点继续消费。
- 一致性保证：
  - **已确认结果不重复、不回退**：恢复重放相同或更旧 `Revision` 都被 Sink 忽略。
    即检查点之前已确认过的结果，恢复后不会再给下游出现第二遍。
  - **修正进度不倒退、不重复**：`Fired/Revision` 随检查点持久化，恢复后在原修订号上继续。
  - **未确认结果不丢失**：崩溃时未确认的输出（含修正）随检查点持久化，恢复后必然重发；
    Sink 按 `(ID,Revision)` 幂等，重发最多生效一次。
  - **恢复后再到迟到数据能正确接续**：窗口只要尚未到达回收线，恢复后仍可继续修正；
    接收边界与回收边界的判定与恢复前完全一致——越过 `eventTime+AllowedLateness`
    的事件即使窗口仍开着也照样拒绝，允许延迟内的事件在持久化的 `Revision` 上继续修正。
  - 恢复后水位线按检查点中的组合水位线还原，因此"哪些事件算迟到"在重启前后保持一致。

### 背压与有界并发

- 每个分片一个摄取 goroutine，经无缓冲通道**逐事件交付**；所有状态写入由唯一消费者
  按 round-robin 串行归并，保证状态一致与结果（含修正版本）确定。
- 已拉取未处理事件受 `InFlightLimit` 信号量约束；内存占用有界，约为
  `InFlightLimit + Shards × FetchBatch` 条事件。
- 下游变慢时，冲刷（flush）阻塞会自然传导为摄取阻塞；`ctx` 取消（或归并器提前退出）时，
  所有阻塞点（拉取、信号量获取、发送、Sink 写入）都可中断，许可不会泄漏，
  无 goroutine 泄漏、无事件泄漏。

## 配置项

| 配置 | 默认 | 说明 |
|---|---|---|
| `WindowSize` | 1m | 滚动窗口大小 |
| `AllowedLateness` | 0 | 允许延迟（按**事件时间**判定，不随窗口大小放大）：`eventTime+AllowedLateness <= 水位线` 的输入按迟到拒绝；窗口状态仍保留到窗口回收线 `End+AllowedLateness` |
| `FetchBatch` | 16 | 单次拉取最大事件数 |
| `InFlightLimit` | 64 | 在途事件上限（背压阈值） |
| `CheckpointEvery` | 32 | 每处理 N 条事件做一次检查点 |
| `CheckpointStore` | nil | 检查点存储（`FileCheckpointStore` 落盘） |
| `Logger` | nil | 审计日志回调：水位线、修正、回收、检查点、背压判定依据 |

`Stats` 新增观测项：`Corrections`（迟到修正输出条数）、`DedupGC`（安全回收的去重 ID 数）。

## 使用示例

```go
cfg := stream.Config{
    WindowSize:      10 * time.Second,
    AllowedLateness: 10 * time.Second, // 落后水位线不超过 10s 的迟到数据会产生修正，超过即丢弃
    InFlightLimit:   64,
    CheckpointStore: &stream.FileCheckpointStore{Path: "state/cp.json"},
    Logger:          func(f string, a ...any) { log.Printf(f, a...) },
}
rt := stream.NewRuntime(cfg, source, sink) // sink 按 (ID, Revision) 幂等
if ok, err := rt.Recover(ctx); err != nil {
    // ErrIncompatibleCheckpoint（旧版/新版）或 ErrCorruptCheckpoint：明确失败，需人工介入
    return err
} else if ok {
    log.Println("已从 v2 检查点恢复")
}
if err := rt.Run(ctx); err != nil { /* 取消或上游错误，检查点已尽力保存 */ }
```

## 验证方法

```bash
# 全部测试，日志含水位线/接收截止线/窗口关闭线/修正/回收/检查点/背压依据
go test ./stream/ -v
# 只复现迟到接收边界（含恢复边界与多分片确定性矩阵）
go test ./stream/ -v -run 'TestLateEventBeyondAllowedLatenessRejected|TestAllowedLatenessNotScaledByWindow|TestLateBoundary'
# 竞态检测 + 多遍复验，确认无死锁/无竞态/结果稳定
go test ./stream/ -race -count=5
```

修正与长期运行相关的测试覆盖：

- `TestLateEventBeyondAllowedLatenessRejected`：**核心边界复现**——水位线超过
  "事件时间 + 允许延迟"后，送一条属于尚未彻底关闭窗口的旧事件，确认它被按迟到拒绝，
  处理量、修正数与下游结果都不受影响。
- `TestAllowedLatenessNotScaledByWindow`：同样的"落后水位线 6s、允许延迟 5s"，
  在 10s / 10min 窗口、事件位于窗口前部/尾部/深处等组合下，接收/拒绝恒由
  `eventTime+AllowedLateness` 决定，允许延迟不被窗口大小放大；允许延迟内的迟到继续修正。
- `TestLateBoundaryDeterministicAcrossShardingAndBatches`：两分片流中注入允许延迟内修正
  与越界拒绝事件，在 3 种 `FetchBatch/InFlightLimit` 配置 × 多轮调度下，
  每个输出的修订版本序列（版本号与每版累计值）与拒绝计数完全一致。
- `TestLateBoundaryAfterRecovery`：检查点恢复后，已确认修订不重发、进度不倒退，
  允许延迟内迟到继续递增修订，越界事件（窗口未关闭）照样拒绝。
- `TestLateCorrectionConverges`：同一窗口被多次迟到数据修正，`Revision=1..N` 单调、
  数值逐步收敛到与"一次性算完"一致的终值。
- `TestCorrectionRetryIdempotent`：同版重发去重、旧版晚到不回退、整版覆盖不累加。
- `TestCorrectionOrderDeterministic`：多分片下多次运行的修订序列完全一致。
- `TestStateSizeBounded` / `TestStateSizePeakBounded`：长时间大流量结束状态归零、
  回收量等于处理量；中途窗口/去重峰值为与总时长无关的常量（8 倍流量峰值不变），
  验证收紧接收边界后回收边界没有变松。
- `TestCorrectionRecovery`：崩溃恢复后修订进度不倒退/不重复，恢复后的迟到数据继续正确修正。
- `TestWindowRevisionSnapshotRestore`：`Fired/Revision` 经检查点快照往返完整保留。
- `TestCheckpointV1RejectedEndToEnd` / `TestCheckpointCorruptAndIncompatible` /
  `TestRecoverFromCorruptCheckpoint`：旧版（v1）、更新版本与损坏检查点都被明确拒绝。

原有语义无回归：

- `TestOutOfOrderConsistency` / `TestDeterministicAcrossRuns`：乱序与多次运行结果一致。
- `TestDuplicateEvents`：重复投递全部去重。
- `TestLateEventDropped`：窗口彻底关闭后的迟到事件拒绝且不计入。
- `TestCheckpointRecovery`：崩溃恢复，已确认不重复、未确认不丢失。
- `TestSlowSinkBackpressure` / `TestCancelNoLeak`：慢下游背压、结果一致、内存有界、取消无泄漏。
