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

是否接收一条迟到数据，按**事件级迟到线**判定（严格对照配置，不被窗口大小放大）：

- **接收并产生修正**：`事件时间 + AllowedLateness > 水位线`
  （即该事件落后水位线还不到允许延迟）。窗口在首次触发
  （`End <= 水位线`）之后、彻底关闭之前收到这样的迟到数据，会并入同一 `(窗口, 键)`
  并产出**修正结果**。
- **直接拒绝**：`事件时间 + AllowedLateness <= 水位线`
  （落后水位线已达到/超过允许延迟）。事件计入 `Stats.LateDropped`，**不修改任何聚合状态、
  不产生任何新结果**——即使它所属的窗口此刻还没有彻底关闭（见下条窗口关闭线）。

注意区分两条边界，二者相差最多一个窗口长度，用途不同、不可混用：

| 边界 | 公式 | 用途 |
|---|---|---|
| 事件级迟到线 | `事件时间 + AllowedLateness` 对水位线 | 判定**每一条输入**接收还是拒绝 |
| 窗口关闭线 | `窗口 End + AllowedLateness` 对水位线 | 判定**窗口状态**何时彻底清理 |

也就是说：窗口首次触发后，允许延迟是**相对当前水位线、按事件自身的事件时间**度量的，
而不是“窗口结束后再多等一个 AllowedLateness”。例如窗口 `[0,10)`、允许延迟 5s：
水位线推进到 11 时（窗口关闭线 15 未到，窗口仍开着），事件时间 6 的旧事件
`6+5=11 <= 11` 已经落后水位线整整 5s，会被拒绝；而事件时间 7 的旧事件
`7+5=12 > 11` 仍在允许延迟内，会继续产生修正。这样允许延迟的范围不会因为窗口大
或分片不同而被放大。

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
**分片轮转（round-robin，按分片号与位点）**串行归并，处理顺序只取决于输入，与 goroutine
调度、到达先后无关。因此每次修正的 `Revision` 及其先后顺序在多分片并发下也是确定的。

> 内置 `MemSink` 已实现上述“同版去重、旧版不回退、整版覆盖”的幂等语义，可直接作为参考实现。

### 状态清理的安全边界（长期在线、规模有界）

长期运行下，两类状态都在水位线推进时持续回收，规模稳定在与
“活跃窗口跨度 × 键数”相关的**常量级**，不随运行天数 / 总事件量增长。
状态回收统一使用**窗口关闭线**（`窗口 End + AllowedLateness <= 水位线`），
与上面“每条输入是否接收”的事件级迟到线是两条独立边界：

- **窗口状态**：窗口在窗口关闭线到达时删除。此时代入该窗口任意事件时间
  `t < End` 都有 `t + AllowedLateness < End + AllowedLateness <= 水位线`，
  即事件级迟到线早已全部拒绝属于该窗口的输入，删除状态不可能影响任何结果。
- **去重集合**：某事件 ID 所属窗口到达窗口关闭线后，才回收该 ID（`DedupStore.CollectBefore`）。

安全边界非常明确：

1. 只要窗口尚未彻底关闭，属于它、且仍在事件级迟到线内的迟到事件仍可能改变结果，
   其窗口聚合状态与相关事件 ID **都不会被清理**——因此不会把迟到数据算错。
2. 窗口彻底关闭后，属于它的任何事件都会被按 `RejectLate` 拒绝、不计入聚合；此时即使其
   去重 ID 已被回收，重复投递的副本也只会被当作迟到拒绝，**不会被重复计入**。

收紧事件级迟到判定**不会也不需要**改变回收边界：窗口状态与去重 ID 的驻留时长仍只取决于
窗口关闭线（最长 `窗口大小 + AllowedLateness`），状态峰值与总事件量无关，长期运行规模稳定。
可通过 `Runtime.StateSizes()` 观察 `(窗口状态数, 去重 ID 数)`，回收量计入 `Stats.DedupGC`。

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
  - **修正进度不倒退、不重复**：`Fired/Revision` 随检查点持久化，恢复后在原修订号上继续。
  - **未确认结果不丢失**：崩溃时未确认的输出（含修正）随检查点持久化，恢复后必然重发。
  - **恢复后再到迟到数据能正确接续**：恢复出的水位线继续按事件级迟到线
    （`事件时间 + AllowedLateness` 对水位线）判定——仍在允许延迟内的迟到数据在已持久化的
    `Revision` 上继续 `+1` 修正；越界旧事件（哪怕其窗口尚未彻底关闭）照样 `RejectLate` 拒绝，
    既不补发结果，也不把修订进度拉回。恢复出的去重集合仍能认出崩溃前已处理的事件。

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
| `AllowedLateness` | 0 | 允许延迟：`事件时间+AllowedLateness <= 水位线` 的输入按迟到拒绝；仍在允许延迟内的迟到数据会产生修正（与窗口大小无关；窗口状态在 `End+AllowedLateness` 时才清理） |
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
    AllowedLateness: 10 * time.Second, // 事件时间+10s 仍晚于水位线的迟到数据会产生修正；越线即拒绝（与窗口大小无关）
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
go test ./stream/ -v                    # 全部测试，日志含水位线/修正/回收/检查点/背压依据
go test ./stream/ -race -count=5        # 竞态检测 + 多遍复验，确认无死锁/无竞态/结果稳定
```

修正与长期运行相关的测试覆盖：

- `TestLateCorrectionConverges`：同一窗口被多次迟到数据修正，`Revision=1..N` 单调、
  数值逐步收敛到与“一次性算完”一致的终值。
- `TestLateBeyondAllowedLatenessRejectedWhileWindowOpen`：**迟到边界**——先把水位线推到
  越过“事件时间+允许延迟”，再送一条属于**尚未彻底关闭窗口**的旧事件，确认它被按
  `RejectLate` 拒绝（`LateDropped` 计数、不计 `Processed`），聚合不变、不补发修正；
  同场景下仍在允许延迟内的迟到数据继续产出单调修正并收敛。
- `TestLateBoundaryMultiShardDeterministic`：多分片并发下，用 4 种不同的
  `FetchBatch / InFlightLimit / CheckpointEvery` 各跑多遍，每个 `(窗口,键)` 的修订序列
  完全一致，越界迟到稳定拒绝 2 条、终值不受污染、末态状态归零。
- `TestLateBoundaryRecovery`：崩溃恢复后，恢复出的水位线继续按事件级迟到线拒绝越界旧事件、
  允许延迟内迟到在已持久化的 `Revision` 上继续修正，去重记录与修订进度不丢不倒退。
- `TestCorrectionRetryIdempotent`：同版重发去重、旧版晚到不回退、整版覆盖不累加。
- `TestCorrectionOrderDeterministic`：多分片下多次运行的修订序列完全一致。
- `TestStateSizeBounded` / `TestStateSizePeakBounded`：长时间大流量结束状态归零、
  回收量等于处理量；中途窗口/去重峰值为与总时长无关的常量（8 倍流量峰值不变），
  验证收紧迟到判定后回收边界没有变松。
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
