# 幂等任务队列与消费位点管理（本地、无外部中间件）

一套基于 Spring Boot 3 / Java 21 的幂等任务队列实现，可在本地直接运行与验证，
不依赖 Kafka / Redis / 数据库等任何外部中间件。它解决五类问题：

1. 同一业务键的**重复投递去重**（并发下也成立）；
2. **失败按策略重试**，耗尽后进入可查询的终态；
3. **消费位点单调管理**，回退被拒绝，消费重启后不重放、不丢失；
4. **容量与速率约束**下拒绝新任务，拒绝原因与业务校验失败明确可区分；
5. **本地持久化与完整重启恢复**（可选，追加式日志 + 快照，见第 7 节）：
   进程彻底停掉再拉起后，未完成任务不丢、已完成不重跑、位点/去重/容量口径不变。

默认仍是纯内存模式（与旧版行为一致）；配置 `idempotent.queue.persistence-dir`
后启用本地持久化，无需任何外部组件。

## 1. 快速开始

```bash
# 运行全部测试（25 个，含并发、重投、重试耗尽、位点回退、完整重启恢复、撕裂写恢复、收拢及收拢中途被杀场景）
mvn test

# 启动应用（自动启动单分区消费 worker）
mvn spring-boot:run

# 启用本地持久化启动（完整重启后从同一目录恢复）
mvn spring-boot:run -Dspring-boot.run.arguments=--idempotent.queue.persistence-dir=./data/queue
```

投递一个任务：

```bash
curl -X POST http://localhost:8080/api/queue/tasks \
  -H 'Content-Type: application/json' \
  -d '{"businessKey":"order-1001","type":"email","payload":"welcome"}'
```

查询：

```bash
curl http://localhost:8080/api/queue/tasks                 # 全部任务视图
curl http://localhost:8080/api/queue/tasks/1               # 按 ID 查询（终态也可查）
curl http://localhost:8080/api/queue/stats                 # 容量/各状态计数/位点/拒绝计数
```

## 2. 队列语义（Queue Semantics）

- **单分区、单 worker、严格 FIFO**：任务按入队分配的单调 ID 升序消费，
  可观察的执行顺序等于投递顺序；队首处于退避/投递中时不会跳过后续任务，避免顺序错乱。
- **投递语义为 at-least-once**：worker 先执行业务副作用，再 ACK；
  ACK 前崩溃会在投递超时后重投同一任务。框架提供幂等守卫
  （`TaskContext.sideEffectDone()` / `markSideEffectDone()`），
  业务处理器据此保证副作用只生效一次。
- **状态机**：

  ```
  PENDING ──claim──> DELIVERING ──ack成功──> SUCCEEDED            （终态）
                        │  ▲
                        │  └──── 投递超时未ACK，重新claim（attempts+1，staleDeliveries+1）
                        └──nack──> RETRY_WAITING（退避到期）──claim──> DELIVERING
                                          └──重试耗尽──> FAILED_PERMANENT（终态，可查询）
  ```

- 终态记录（成功/永久失败）保留在内存中：用于业务键永久去重与状态查询，
  不再参与调度与容量计数。

## 3. 去重规则（Deduplication）

- 以 `businessKey` 为业务幂等键，**去重覆盖全生命周期**（任务成功或永久失败后仍拒绝重复键）。
- “检查业务键 → 容量检查 → 插入”在 `TaskStore` 的**单一锁**内原子完成，
  因此并发重复投递中**恰好 1 个请求被接受**，其余返回
  `DUPLICATE_BUSINESS_KEY` 并携带**既有任务 ID**，便于关联与解释。

## 4. 拒绝原因（可区分、可解释）

入队判定顺序固定，错误码互斥：

| 错误码 | 含义 | HTTP | `capacityReject` |
|---|---|---|---|
| `INVALID_TASK` | businessKey/type 为空等**业务校验**失败 | 400 | false |
| `DUPLICATE_BUSINESS_KEY` | 业务键已存在（含终态） | 409 | false |
| `RATE_LIMITED` | 超过令牌桶速率（默认 100/s，桶容量=1 秒突发） | 429 | true |
| `QUEUE_FULL` | 在队未终态任务达到容量上限（默认 1000） | 429 | true |

容量/速率拒绝**不写入任何存储状态、不触碰位点**，因此不影响已被接受的排队任务。
统计接口分别暴露 acceptedTotal / rejectedDuplicateTotal / rejectedInvalidTotal /
rejectedCapacityTotal。

## 5. 重试规则（Retry）

- 默认策略为**指数退避 + 可复现抖动**：`backoff = base * 2^(attempt-1)`（上限 5s），
  抖动由任务 ID 与尝试次数确定性推导（避免并发重试同时唤醒，且可解释、可复现）。
  另提供固定间隔策略 `FixedIntervalRetryPolicy`。
- 最大尝试次数 `maxAttempts`（默认 3，含首次）：`attempts < maxAttempts` 时进入
  `RETRY_WAITING`，`nextVisibleAt` 为下次可见时间（退避窗口内不可领取）；
  `attempts == maxAttempts` 仍失败则进入 **`FAILED_PERMANENT` 终态**，
  记录最后错误信息、失败时间、尝试次数，可通过 GET /tasks/{id} 查询。
- 永久失败同样按顺序推进位点，后续任务不会因此丢失。

## 6. 消费位点与兼容范围（Offset Compatibility）

位点表示“已完成并提交的最后一个任务 ID”，从 0 开始，下一个待处理任务为 `committed+1`。
终态迁移与位点推进在同一把存储锁内原子完成。`OffsetManager.commit(requested)` 规则：

| 请求 | 判定 |
|---|---|
| `requested < committed` | **拒绝 `REGRESSION`**（位点回退不允许，绝不从中间继续） |
| `requested == committed` | 接受，幂等空操作（重复提交安全） |
| `requested > committed+1` | **拒绝 `GAP`**（跳过相邻任务形成缺口） |
| `requested == committed+1` 且相邻任务已终态 | 接受，位点前进 1 |
| `requested == committed+1` 但相邻任务未终态 | 拒绝 `GAP` |

- 重复 ACK：任务已终态时为空操作，不重复推进位点、不重复执行副作用。
- 消费进程/worker 重启：已提交位点之前的任务（终态）在扫描时跳过，
  从 `committed+1` 继续，不重复、不丢失。
- ACK 前崩溃：任务停留在 `DELIVERING`，超过 `delivery-timeout` 后被同一任务重新领取
  （`staleDeliveries` 计数 +1，便于解释重复消费），依靠幂等守卫避免副作用重复。

## 7. 本地持久化与完整重启恢复（可选）

配置 `idempotent.queue.persistence-dir` 后启用。同一目录下“进程彻底停掉 → 新进程拉起”，
之前已接受未处理完的任务、待处理顺序、消费位置全部接上：不丢、不重跑、口径不变。

### 7.1 存储结构与写入规则

目录下两个文件：

| 文件 | 作用 |
|---|---|
| `journal.log` | 追加式日志（WAL）：每次状态变更先落盘再生效（帧 = 长度前缀 + CRC32 + 事件体） |
| `snapshot.dat` | 收拢产生的全量快照（写 `snapshot.tmp` → fsync → 原子 rename） |

关键写入规则：

- **终态迁移与位点推进写在同一条 TERMINAL 帧里**：两者要么同时生效、要么同时不生效，
  恢复后绝不会出现“位点已前进、对应任务却没结束”或“任务已结束、重启后又冒出来”。
- **副作用幂等标记先落盘、再置内存标记**：handler 调 `markSideEffectDone()` 后、
  ACK 前被杀，重启重投仍能跳过副作用（exactly-once 副作用语义跨进程保持）。
- 去重/容量拒绝也记日志（各一条小帧），因此 `acceptedTotal`、
  `rejectedDuplicateTotal`、`rejectedCapacityTotal` 等计数重启后连续。
  注意：`INVALID_TASK` 与 `RATE_LIMITED` 拒绝不触碰存储，属于进程级指标，重启后清零。
- **REJECT 帧携带全局单调递增的 `rejectSeq`**：拒绝计数不依附任何任务记录，
  无法复用记录版本号做幂等，因此每条拒绝事件带序号；快照记录已覆盖的
  `lastRejectSeq` 高水位，重放时 `rejectSeq <= lastRejectSeq` 的事件跳过。
  这是“收拢中途被杀不重复计数”的持久化依据（见 7.3）。

### 7.2 恢复规则与判断依据

启动时（`QueuePersistence.open`）：

1. 丢弃未 rename 的 `snapshot.tmp`（上次收拢中途被杀的残留，正式快照仍完整）；
2. 校验并装载 `snapshot.dat`（magic + 格式版本 + CRC32）；
3. 按序重放 `journal.log` 的 CRC 校验帧，幂等应用：
   - 任务事件按“事件版本号 > 记录当前版本才应用”去重；
   - REJECT 事件按“`rejectSeq` > 快照 `lastRejectSeq` 高水位才应用”去重
     （收拢中途崩溃会造成快照与日志重叠，重叠事件被跳过并计入恢复报告的
     `journalEventsSkipped`，绝不重复计入拒绝计数）；
4. 帧尾处理：
   - **帧不完整（撕裂写）** → 截断到最后一条完整帧，恢复为“已发生历史的一个一致前缀”，
     继续运行；
   - **帧完整但 CRC 不符 / 长度非法，或快照校验失败** → 抛 `RecoveryException`
     **拒绝启动**，异常消息说明损坏位置与判断依据，不悄悄糊过去；
   - **旧格式歧义（v1 快照 + 无序号 REJECT 帧）** → 无法判定该次拒绝是否已计入
     快照计数（计入会重、不计会漏），同样抛 `RecoveryException` **拒绝启动**，
     消息说明判断依据与人工处置方式（离线核对计数，或确认历史可弃后重置目录），
     不会悄悄选一个口径继续对外服务；
5. 恢复全程输出 `[RECOVERY]` 日志（快照任务数、重放帧数、跳过帧数、截断位置、
   恢复后位点），也可通过 `GET /api/queue/recovery` 查看最近一次恢复报告
   （进度/位点/依据）。

恢复后口径：未终态任务继续占容量、终态不占；已用业务键继续拦截重复投递；
`DELIVERING` 中的任务按既有投递超时机制重投（僵死重投语义不变）。

### 7.3 历史收拢（compaction）

- **触发**：日志超过 `journal-compact-threshold-bytes`（默认 1MB）自动收拢；
  或手动 `POST /api/queue/compact`。
- **过程**：在存储锁内取全量状态写快照（原子替换），再清空日志；
  不影响正常投递与消费。
- **收拢中途被强杀的重启一致性规则**（这条路径不是天然成立的，依赖以下机制）：
  收拢不是单个原子动作，崩溃点落在不同阶段时磁盘状态不同，恢复规则分别为：
  1. 写在 `snapshot.tmp` 阶段被杀 → tmp 残留直接丢弃，正式快照与日志都还是旧的，完整一致；
  2. **快照已 rename、日志尚未清空**被杀 → 磁盘上快照与日志重叠：
     任务事件靠记录版本号、拒绝计数靠 `rejectSeq` 高水位幂等去重，
     重启后拒绝计数（重复投递/容量）、各状态数量、消费位点、业务键查重结果
     与停机前**完全一致**（恢复报告 `journalEventsSkipped` 可见被跳过的重叠帧数）；
  3. 日志清空完成后被杀 → 等价于正常收拢完成。
- **影响**：收拢前后语义完全一致（状态/位点/去重/计数）。注意业务键去重是**永久**的，
  因此终态任务的键会随快照保留（这是去重语义的必要代价），收拢压缩的是日志中的
  历史变更记录。收拢中断不会让它空转：恢复后下一次收拢照常清空日志，
  本地存储体积仍受控。

### 7.4 兼容范围

- **第一次启动**：目录不存在则自动创建，从空状态开始。
- **纯内存 → 持久化**：旧版本不产生任何本地数据，不存在迁移问题；
  不配置 `persistence-dir` 时行为与旧版完全一致（既有 15 个用例不回归）。
- **格式版本**：快照格式版本为 2（v2 起携带 `lastRejectSeq` 高水位；v1 快照仍可读取），
  REJECT 日志帧自 v2 起携带 `rejectSeq`（旧格式无序号帧按 rejectSeq=0 识别）。
  遇到不支持的版本或校验失败即拒绝启动，不会以损坏状态继续运行；
  v1 快照与旧格式 REJECT 帧同时出现属于无法确定计数口径的歧义状态，同样拒绝启动
  （见 7.2）。

## 8. 配置项（application.properties）

| 键 | 默认值 | 说明 |
|---|---|---|
| `idempotent.queue.capacity` | 1000 | 在队未终态任务容量 |
| `idempotent.queue.rate-limit-per-second` | 100.0 | 投递速率（令牌桶） |
| `idempotent.queue.max-attempts` | 3 | 最大消费尝试次数（含首次） |
| `idempotent.queue.retry-backoff-millis` | 50 | 首次失败退避基数（指数增长） |
| `idempotent.queue.delivery-timeout-millis` | 5000 | DELIVERING 多久未 ACK 判定僵死并重投 |
| `idempotent.queue.auto-start-worker` | true | 是否随应用启动消费 worker |
| `idempotent.queue.persistence-dir` | 空（关闭） | 本地持久化目录；配置后启用日志+快照 |
| `idempotent.queue.journal-compact-threshold-bytes` | 1048576 | 日志超过该字节数自动收拢为快照 |

## 9. 验证方法（测试覆盖）

测试均为本地内存运行，日志以 `[TEST-INPUT] / [TEST-OFFSET] / [TEST-RESULT] /
[TEST-BASIS]` 前缀打印**输入、位点与判定依据**：

| 测试类 | 覆盖场景 |
|---|---|
| `ConcurrentEnqueueDedupTest` | 32 线程并发投递同一业务键：恰好 1 接受，其余全部 DUPLICATE |
| `FifoAndOffsetTest` | FIFO 顺序、位点随终态推进、重复 ACK 不重放 |
| `RetryExhaustionTest` | 退避后重试成功；耗尽后进 FAILED_PERMANENT 且位点继续推进、后续任务不丢 |
| `OffsetRejectTest` | 位点回退 REGRESSION、跳跃 GAP、未终态不可越过、重复提交幂等 |
| `CapacityAndRateLimitTest` | QUEUE_FULL / RATE_LIMITED / INVALID_TASK 可区分；拒绝不污染队列 |
| `CrashRedeliveryRestartTest` | ACK 前崩溃重投副作用仅一次；消费重启从 committed+1 恢复 |
| `ConcurrentProduceConsumeTest` | 12 生产者 × 80 键 × 3 份并发投递 + 后台消费：无丢失、无重复、位点连续 |
| `QueueWebIntegrationTest` | 真实容器端到端：201/409/400 错误码与 stats 判定依据 |
| `PersistenceRestartTest` | 同一目录重建实例模拟完整重启：已完成不重跑、未完成不丢且 FIFO 不乱、位点/各状态计数/去重/容量口径一致；副作用标记跨重启保留 |
| `CrashMidWriteRecoveryTest` | 日志帧尾撕裂→截断为一致前缀并可继续；完整帧损坏/快照损坏→拒绝启动并说明依据 |
| `CompactionTest` | 手动/自动收拢前后语义一致；收拢后重复投递、失败重试、重启恢复均正常 |
| `CrashMidCompactionRecoveryTest` | 收拢在“快照已 rename、日志未清”之间被强杀→重启后拒绝计数/各状态数量/位点/查重与停机前完全一致，恢复后新拒绝、重试、消费、再次收拢照常；旧格式歧义状态→拒绝启动并说明依据 |

```bash
mvn test                                  # 全部
mvn -Dtest=OffsetRejectTest test          # 单场景
mvn -Dtest=PersistenceRestartTest test      # 完整重启恢复
mvn -Dtest=CrashMidWriteRecoveryTest test  # 写一半被杀后的恢复
mvn -Dtest=CompactionTest test            # 历史收拢
mvn -Dtest=CrashMidCompactionRecoveryTest test  # 收拢中途被强杀后的重启一致性
```

本地手动验证完整重启：

```bash
# 终端1：启用持久化启动，投递几个任务后 Ctrl-C 杀掉
mvn spring-boot:run -Dspring-boot.run.arguments=--idempotent.queue.persistence-dir=/tmp/q
curl -X POST localhost:8080/api/queue/tasks -H 'Content-Type: application/json' \
  -d '{"businessKey":"a","type":"t"}'
# 再次用同一目录启动，观察 [RECOVERY] 日志与 /api/queue/recovery、/api/queue/stats
# 已完成任务不会重跑，未完成任务继续被消费，位点接续
```

本地验证“收拢中途被强杀”路径（手工杀进程难以精确命中 rename 与清日志之间的窗口，
确定性复现走自动化测试的崩溃钩子）：

```bash
mvn -Dtest=CrashMidCompactionRecoveryTest test
# 观察日志：
#   [TEST-INPUT]  停机前的拒绝计数/各状态数量/位点，以及强杀后磁盘上的重叠状态
#                 （snapshot.dat 已存在、journal.log 未清空）
#   [TEST-OFFSET] 重启恢复报告：framesReplayed / journalEventsSkipped / committed
#   [TEST-RESULT] 重启前后计数逐项相等的断言结果；恢复后继续投递/消费/再次收拢
#   [TEST-BASIS]  判断依据：rejectSeq 高水位 + 记录版本号幂等
# 判定标准：重启后 rejectedDuplicateTotal / rejectedCapacityTotal 与停机前完全一致，
# 恢复报告中 journalEventsSkipped > 0（重叠帧被跳过而非重复计入）。
```

## 10. 关键设计取舍

- **本地文件而非外部中间件**：默认纯内存；可选的本地持久化用“追加日志 + 原子快照”
  实现完整重启恢复，不引入任何外部组件。语义正确性由单锁 + 单调位点 + 幂等标记 +
  “终态与位点同帧”+ “拒绝计数 rejectSeq 高水位”保证。
- **严格 FIFO 优先于吞吐**：单分区单线程，退避中的队首会短暂阻塞后续任务，
  以换取可观察顺序的确定性。
- **永久去重窗口**：终态键永久保留（启用持久化后随快照跨进程保留）；
  容量计数只含未终态任务，因此历史终态不占用队列容量。
