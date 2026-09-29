# stream-state-runtime-go

可嵌入式的事件时间流处理运行时。面向乱序、重复投递、进程重启与慢下游场景，
保证处理结果一致且可审计，全部行为可在本地复现与验证。

## 处理语义

### 事件时间窗口与水位线

- 事件按 `EventTime` 分配到左闭右开的滚动窗口 `[Start, End)`（`WindowSize` 对齐到 Unix 纪元）。
- 每个上游分片独立维护水位线，组合水位线取各分片最小值；**水位线只单调推进**，回退被忽略。
- 窗口 `End <= 水位线` 时**首次触发**（`Revision=1`）；状态保留到
  `End + AllowedLateness <= 水位线`，期间容忍期内的迟到数据产生版本递增的
  修正结果（详见下文“迟到数据修正”）。
- 迟到判定：`EventTime <= 水位线 - AllowedLateness` 的事件被拒绝（`RejectLate`），不计入聚合，
  拒绝原因写入日志与 `Stats.LateDropped`。
- 重复投递：同一 `Event.ID` 只计入一次（`DedupStore`），重复事件计入 `Stats.Duplicates`。
- 结束语义：所有分片耗尽后，水位线推进到正无穷，触发全部剩余窗口，结果确定且可重放。

### 迟到数据修正（版本化结果 / upsert）

窗口在 `End <= 水位线` 时**首次触发**（`Revision=1`），但状态不会立即删除，而是
保留到迟到容忍期结束（`End + AllowedLateness`）。这期间到达的容忍期内迟到数据
会更新同一窗口，并向下游补发**同一 `ID`、版本号 +1** 的修正结果：

- **下游语义是 latest-wins（upsert），不是累加**：`Output.ID` 对同一 `(窗口, 键)`
  恒定，`Output.Revision` 从 1 起严格单调递增；下游应保留同一 ID 中
  `Revision` 最大的一份，旧版本（包括因重试晚到的旧版本）必须忽略。
- **同版本重发幂等**：相同 `(ID, Revision)` 因 at-least-once 重试/恢复重发出现
  多次时，下游只生效一次，绝不累加、绝不多出结果。
- **版本号与到达顺序无关**：最终 `Revision = 1 + 进入已触发窗口的迟到事件数`，
  因此同一批数据无论以什么顺序到达、中间修正几次，最终每个 `(窗口,键)` 的
  `Sum/Count/Revision` 都与“所有数据一次性算完”完全一致；中间修正的产生与
  写出都在单锁下串行，版本先后确定，多分片并发也不会出现版本号倒挂。
- 超出容忍边界（`EventTime <= 水位线 - AllowedLateness`）的事件仍被拒绝
  （`RejectLate`），不再产生修正；窗口一旦过容忍期，其结果永久不可变。
- 运行时在写 Sink 前会用“已确认最高版本账本”（见下）过滤：已被更高版本取代
  或已确认过的输出不会再发给下游。

### 长期运行的状态清理与安全边界

状态规模只与“水位线附近仍有可能改变结果的时间跨度”有关，**不随已处理事件
总量增长**；每次落盘的检查点内容同样有界（`Runtime.StateSize()` 可观测
`Windows / Dedup / Pending / Acks` 四类状态的实时规模）。清理在每次水位线
推进（以及重复事件到达）时进行，边界清晰可验证：

| 状态 | 清理时刻（水位线 `W`） | 为什么安全 |
|---|---|---|
| 窗口聚合状态 | `窗口.End + AllowedLateness <= W` | 此刻能进入该窗口的事件都已被判为不可接受的迟到，结果再不可能改变 |
| 去重事件 ID | 与该 ID 所属窗口同生命周期，`End + AllowedLateness <= W` | 窗口结果未不可变前，重复投递必须仍被认出是 duplicate；过线后重复件只会被判迟到，删除 ID 不会造成重复计入 |
| 已确认版本账本 (`Acks`) | 对应窗口已清理且无同 ID 在途输出 | 窗口不会再有新版本，账本条目不再需要用于拦截旧版本重放 |

- 边界**之内**一律保留：还可能修正结果的窗口状态、还需要识别重复投递的事件 ID。
- 水位线只单调推进，因此清理只朝一个方向进行，不会出现“清理后又需要”的情况。

### 检查点与恢复

检查点（`Checkpoint`）涵盖六类状态，构成一致性边界：

| 状态 | 内容 |
|---|---|
| 消费位点 | 每个分片下一条待消费的 offset |
| 窗口状态 | 未清理窗口的聚合中间结果，含 `Fired`/`Revision`（修正到哪一步） |
| 去重集合 | 已处理事件 ID 及其所属窗口的清理时刻 |
| 水位线 | 组合水位线（单调推进） |
| 待确认输出 | 已触发但尚未成功写入 Sink 的输出（含各修正版本） |
| 确认账本 Acks | 每个输出 ID 已被下游确认的最高 `Revision`（修正进度） |

- 落盘格式：`JSON(数据) + SHA-256 校验和`，临时文件 + rename 原子提交，带格式版本号。
- **版本兼容范围（当前格式版本 = 2）**：
  - v2：支持版本化修正、状态安全清理与确认账本；
  - v1（触发即删窗口、纯 ID 去重、无版本/账本）：**明确拒绝**，错误说明
    “来自旧版本、低于最低支持版本”，不会被当成新状态静默加载；
  - 更高版本（如 v3）：同样**明确拒绝**，错误说明“来自更新版本”，避免新状态被旧程序破坏。
- 恢复流程：加载检查点（校验和 + 版本双检查）→ 恢复位点/窗口/去重/水位线/Acks
  → **用 Acks 过滤 pending** → 先补发仍未确认的输出 → 从位点继续消费。
- 一致性保证：
  - **已确认结果不重复**：`Acks[id]` 记录已确认最高版本，恢复重放时
    `Revision <= Acks[id]` 的输出被直接丢弃，不会再出现一遍；Sink 亦按
    `(ID, Revision)` 幂等。
  - **修正进度不倒退、不重复**：窗口恢复后带着原 `Revision`，新的迟到数据
    从下一版本号继续修正；同版本重发被幂等吞掉。
  - **未确认结果不丢失**：崩溃时未确认的输出（含未确认的修正版本）随检查点
    持久化，恢复后必然重发，且每版恰好一次。
  - **损坏/不兼容明确拒绝**：校验和不匹配返回 `ErrCorruptCheckpoint`，
    版本不匹配返回 `ErrIncompatibleCheckpoint`（错误信息区分旧版本/更新版本），
    绝不静默继续。

### 背压与有界并发

- 每个分片一个摄取 goroutine，并行拉取；处理在单互斥锁下串行化，保证状态一致、
  修正版本确定与结果可重放。
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

测试覆盖（`-v` 可在日志中看到事件时间/水位线/触发/修正/清理/检查点/背压依据）：

**原有行为（防回归）**

- `TestOutOfOrderConsistency`：乱序流结果与参考聚合逐窗口一致
- `TestDuplicateEvents`：重复投递全部被去重，结果不变
- `TestLateEventDropped`：迟到事件拒绝且不计入聚合
- `TestDeterministicAcrossRuns`：并发下多次运行结果完全一致
- `TestCheckpointRecovery`：崩溃后恢复，已确认不重复、未确认不丢失
- `TestCheckpointCorruptAndIncompatible` / `TestRecoverFromCorruptCheckpoint`：损坏与版本不兼容检查点被明确拒绝
- `TestSlowSinkBackpressure`：慢下游下结果一致、内存有界
- `TestCancelNoLeak`：取消后无死锁、无 goroutine 泄漏

**新增行为**

- `TestLateCorrectionsConverge`：同一窗口首次结果发出后，多次容忍期内迟到
  数据产生 `Revision=2/3…` 的修正，下游逐版更新、同版本重试/重复投递不重复，
  超容忍数据被拒，最终收敛到“一次性算完”的结果
- `TestSinkRevisionSemantics`：下游 upsert 语义——同版本幂等、高版本覆盖、
  旧版本拒绝（绝不回退）
- `TestStateSizeStable`：事件时间推进 30 分钟、1.4 万事件（含重复投递）下，
  窗口/去重/账本峰值只由水位线附近时间跨度决定、远小于总事件量，结束时全部
  回收，检查点文件收敛到数百字节
- `TestRevisionRecovery`：修正中途崩溃，已确认版本（`rev<=2`）恢复后不再发送、
  版本不倒退，恢复后迟到数据继续修正到正确终值
- `TestRevisionRecoveryUnconfirmed`：未确认修正版本（rev=2）崩溃后恰好重发
  一次，随后继续修正到终值
- `TestCheckpointVersionReject`：v1（旧版本）与 v3（更新版本）检查点均被
  明确拒绝并说明原因
- `TestWindowFireReady` / `TestDedupGCBoundary`：触发→修正→到期清理生命周期
  与去重 ID 清理边界的单元级验证
