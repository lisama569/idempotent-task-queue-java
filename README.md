# 幂等任务队列（本地运行，无外部中间件）

在 Spring Boot 工程内实现的一套幂等任务队列与消费位点管理能力。任务在
**并发投递、重复投递、失败重试、消费重启** 后仍能得到确定且可解释的结果。
全部状态通过本地追加式 WAL（`build/queue-data/queue.wal`）持久化，无需任何外部中间件。

## 运行与验证

```bash
# 运行全部测试（12 个用例，覆盖并发投递/重复消费/重试耗尽/位点回退等）
mvn test

# 本地启动服务（默认 8080 端口）
mvn spring-boot:run
```

HTTP 接口：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/tasks` | 投递任务 `{businessKey, type, payload, maxAttempts}` |
| GET | `/api/tasks/{businessKey}` | 查询单个任务状态与失败原因 |
| GET | `/api/queue` | 队列快照：提交位点、容量、计数、全部任务 |
| POST | `/api/queue/offset` | 提交消费位点 `{offset}`（演示回退拒绝） |
| POST | `/api/consumer/start\|stop` | 优雅停止/启动消费者（模拟消费进程重启） |

内置演示任务类型：`demo.success`（恒定成功）、`demo.always-fail`（恒定失败，验证重试耗尽）、
`demo.flaky`（前两次失败第三次成功，验证重试恢复）。

## 队列语义

- **严格 FIFO**：任务入队即获得单调递增序号 `seq`（从 1 开始，无空洞）。单线程消费者
  始终取序号最小的可运行任务执行；队首任务处于退避等待时，后续任务不会被越过，
  因此执行顺序与入队顺序一致，可观察、可断言。
- **恰好一次（同进程正常运行）**：同一时刻只有一个任务在执行，成功/失败判定在队列锁内
  完成并先落盘再返回，正常运行期内副作用不会重复执行。
- **至少一次（崩溃恢复）**：进程在任务执行中崩溃时，该任务在 WAL 中停留在 `RUNNING`，
  重启后 `recover()` 将其复位为 `PENDING` 重新投递。因此业务处理器的副作用应设计为幂等。

## 去重与拒绝规则

投递按以下顺序判定（全部在同一把队列锁内，并发下结论确定）：

| 顺序 | 判定 | 拒绝原因（响应 `reason`） | HTTP |
| --- | --- | --- | --- |
| 1 | `businessKey`/`type` 为空或类型未注册 | `VALIDATION_FAILED` | 400 |
| 2 | 业务键已存在（含终态任务） | `DUPLICATE_BUSINESS_KEY` | 409 |
| 3 | 未完成任务数达到 `capacity` | `CAPACITY_EXCEEDED` | 429 |
| 4 | 最近 1 秒接受数达到 `rate-limit-per-second` | `RATE_LIMITED` | 429 |

要点：

- **去重**：同一 `businessKey` 只允许存在一个任务（无论排队中、执行中还是终态）。
  重复投递被明确拒绝，响应中携带已存在任务的 `taskId/seq/status`，不会创建新任务、
  不会消耗序号，副作用不会重复。
- **容量/速率拒绝不影响已接受任务**：拒绝发生在分配序号之前，已排队任务照常消费。
- 所有拒绝原因与业务校验失败在响应体 `reason` 字段与 HTTP 状态码上均可区分。

## 重试规则

- 处理器抛异常视为业务失败，进入 `RETRY_WAIT`，固定退避
  `idempotent.queue.retry-backoff-millis`（默认 200ms）后重新调度。
- 每个任务最多尝试 `maxAttempts` 次（投递时可指定，默认
  `idempotent.queue.default-max-attempts`=3）。
- 第 `maxAttempts` 次失败后进入 **`FAILED` 终态**，不再调度；每次失败原因都记录在
  任务的 `failureReasons` 中，可通过 `GET /api/tasks/{businessKey}` 查询，结果可解释。
- `FAILED` 与 `SUCCEEDED` 一样视为终态，参与位点推进（见下），不会阻塞后续任务。

## 消费位点语义与兼容范围

- **位点定义**：已提交位点 = 已到达终态（`SUCCEEDED`/`FAILED`）的**连续任务前缀**的最大序号。
  任务到达终态时，位点沿连续前缀自动推进并写入 WAL（`OFFSET` 记录）。
- **提交规则**（`POST /api/queue/offset`）：
  - `offset < 当前位点`：**拒绝**，`reason=ROLLBACK`。位点只允许单调前进，
    回退必须被拒绝而不是从中间继续；
  - `offset == 当前位点`：幂等成功（重复消费/重复提交无害）；
  - `offset > 当前连续可推进位点`：**拒绝**，`reason=OUT_OF_RANGE`，不允许跳过未完成任务。
- **兼容范围**：位点兼容于「同一 WAL 日志文件、同一序号空间」内的重启恢复；
  重启后位点从 WAL 恢复，已完成任务不会重复执行，未完成任务从断点继续。
  更换/删除 WAL 文件即视为全新队列，位点从 0 重新开始。
- **重启恢复**：`recover()` 回放 WAL 重建任务表、业务键索引与位点；
  `RUNNING` 任务复位为 `PENDING`（至少一次重投），去重状态与位点不丢不重。

## 配置项（`application.properties`，前缀 `idempotent.queue`）

| 键 | 默认 | 说明 |
| --- | --- | --- |
| `data-dir` | `build/queue-data` | WAL 日志目录 |
| `capacity` | `100` | 未完成任务容量上限 |
| `rate-limit-per-second` | `20` | 每秒接受投递上限（滑动窗口） |
| `default-max-attempts` | `3` | 默认最大尝试次数 |
| `retry-backoff-millis` | `200` | 固定重试退避 |
| `poll-millis` | `20` | 消费轮询间隔 |
| `auto-start-consumer` | `true` | 随应用启动消费者 |

## 测试与验证方法

`mvn test` 运行全部用例；测试日志打印每次判定的**输入、位点与判定依据**
（`[投递输入]/[投递判定]/[消费输入]/[消费判定]/[场景N位点]`），可直接核对。

| 测试类 / 场景 | 覆盖点 |
| --- | --- |
| 场景1 `concurrentSubmitDeduplicatesAndConsumesInOrder` | 16 线程并发投递 200 次（100 键 × 2）：恰好 100 接受 + 100 去重拒绝；序号连续无丢失；每任务恰好执行一次；执行顺序与序号一致 |
| 场景2 `consumerRestartDoesNotDuplicateOrLose` | 优雅停止 → WAL 重建 → 重启：位点恢复、旧任务不重复执行、新任务从断点继续、已完成键重复投递仍被拒 |
| 场景3 `crashDuringExecutionRecoversRunningTask` | 执行中崩溃：`RUNNING` 复位 `PENDING`，恢复后重投并完成（至少一次） |
| 场景4 `offsetRollbackRejectedAndEqualCommitIdempotent` | 位点回退拒绝 `ROLLBACK`、相等提交幂等、越界提交拒绝 `OUT_OF_RANGE` |
| 场景5 `retryExhaustionLeadsToFailedTerminalState` | 恒定失败任务恰好尝试 `maxAttempts` 次进入 `FAILED` 终态，失败原因可查询，位点照常推进 |
| 场景6 `flakyTaskSucceedsAfterRetries` | 前两次失败第三次成功，状态/次数/历史原因可解释 |
| 场景7 `capacityRateAndValidationRejectionsAreDistinguishable` | 容量/速率/校验失败三种拒绝原因可区分，且不影响已排队任务 |
| 场景8 `walRecoveryPreservesStateAndOffset` | 重启后状态、位点、去重索引完整恢复 |
| `QueueControllerTest` | HTTP 层：409/429/400 与 `reason` 字段可区分，位点回退拒绝 |
| `ApplicationTests` | Spring 上下文装配冒烟 |

## 代码结构

```
src/main/java/com/github/lisama569/idempotenttaskqueuejava/
├── model/      TaskRecord / TaskStatus / RejectReason / SubmitResult / OffsetCommitResult / QueueSnapshot
├── storage/    WalStore（JSON 行追加 + fsync）/ WalEntry
├── queue/      IdempotentTaskQueue（去重、容量、速率、调度、位点）/ ConsumerLoop（单线程消费）
├── retry/      RetryPolicy / FixedBackoffRetryPolicy
├── handler/    TaskHandler / TaskHandlerRegistry / 三个演示处理器
├── config/     QueueProperties / QueueAutoConfiguration
└── web/        QueueController / GlobalExceptionHandler / ApiException
```
