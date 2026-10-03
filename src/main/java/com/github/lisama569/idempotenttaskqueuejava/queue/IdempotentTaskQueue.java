package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.*;
import com.github.lisama569.idempotenttaskqueuejava.limit.RateLimiter;
import com.github.lisama569.idempotenttaskqueuejava.retry.RetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.store.RetryDecision;
import com.github.lisama569.idempotenttaskqueuejava.store.TaskStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * 幂等任务队列门面：编排业务校验、去重、速率/容量约束、失败重试与位点提交。
 *
 * <p>拒绝原因判定顺序（每一步原因彼此可区分）：
 * INVALID_TASK（业务校验）→ DUPLICATE_BUSINESS_KEY（重复投递）
 * → RATE_LIMITED（速率）→ QUEUE_FULL（容量，存储层判定）。
 * 容量/速率类拒绝不消耗任何存储状态，因此不影响已接受的排队任务。</p>
 */
public class IdempotentTaskQueue {
    private static final Logger log = LoggerFactory.getLogger(IdempotentTaskQueue.class);

    private final TaskStore store;
    private final RateLimiter rateLimiter;
    private final RetryPolicy retryPolicy;
    private final OffsetManager offsetManager;
    private final QueueProperties properties;
    private final Clock clock;

    private long rejectedInvalidTotal = 0L;
    private long rejectedRateTotal = 0L;
    private java.util.function.BooleanSupplier runningIndicator = () -> false;

    public IdempotentTaskQueue(TaskStore store, RateLimiter rateLimiter, RetryPolicy retryPolicy,
                               OffsetManager offsetManager, QueueProperties properties) {
        this(store, rateLimiter, retryPolicy, offsetManager, properties, Clock.systemUTC());
    }

    public IdempotentTaskQueue(TaskStore store, RateLimiter rateLimiter, RetryPolicy retryPolicy,
                               OffsetManager offsetManager, QueueProperties properties, Clock clock) {
        this.store = store;
        this.rateLimiter = rateLimiter;
        this.retryPolicy = retryPolicy;
        this.offsetManager = offsetManager;
        this.properties = properties;
        this.clock = clock;
    }

    /**
     * 投递任务。
     * 同一 businessKey 的重复投递被明确拒绝（DUPLICATE_BUSINESS_KEY）并返回既有任务ID；
     * 并发投递由 TaskStore 单锁串行化去重判定，至多一个请求被接受。
     */
    public EnqueueResult enqueue(String businessKey, String type, String payload) {
        Instant now = clock.instant();

        String invalid = validate(businessKey, type);
        if (invalid != null) {
            rejectedInvalidTotal++;
            log.info("reject enqueue: reason=INVALID_TASK, businessKey={}, detail={}", businessKey, invalid);
            return EnqueueResult.rejected(RejectReason.INVALID_TASK, null, invalid);
        }

        // 速率限制不改动存储；被拒后已排队任务不受任何影响
        if (!rateLimiter.tryAcquire(clock.millis())) {
            rejectedRateTotal++;
            log.info("reject enqueue: reason=RATE_LIMITED, businessKey={}", businessKey);
            return EnqueueResult.rejected(RejectReason.RATE_LIMITED, null,
                    "rate limit exceeded: max=" + properties.getRateLimitPerSecond() + "/s");
        }

        // 去重 + 容量判定 + 插入，在存储单锁内原子完成
        EnqueueResult result = store.enqueue(businessKey, type, payload,
                properties.getMaxAttempts(), now);
        if (result.isAccepted()) {
            log.info("accept enqueue: taskId={}, businessKey={}, committedOffset={}",
                    result.getTaskId(), businessKey, offsetManager.committed());
        } else {
            log.info("reject enqueue: reason={}, businessKey={}, existingTaskId={}, detail={}",
                    result.getRejectReason(), businessKey, result.getExistingTaskId(), result.getDetail());
        }
        return result;
    }

    private String validate(String businessKey, String type) {
        if (businessKey == null || businessKey.isBlank()) {
            return "businessKey must not be blank";
        }
        if (type == null || type.isBlank()) {
            return "type must not be blank";
        }
        return null;
    }

    public TaskView getTask(long id) {
        return store.findById(id).map(TaskView::from).orElse(null);
    }

    /**
     * 标记业务副作用已执行（幂等守卫落盘点）：在存储锁内更新并触发持久化，
     * 保证“副作用已做但 ACK 前进程被杀”在真实重启后仍不会重复执行副作用。
     */
    public void markSideEffectDone(TaskRecord task) {
        store.withLock((Runnable) () -> store.markSideEffectDone(task.getId()));
    }

    public Optional<TaskView> findByBusinessKey(String businessKey) {
        return store.findByBusinessKey(businessKey).map(TaskView::from);
    }

    public List<TaskView> listTasks() {
        return store.snapshotAll();
    }

    public long committedOffset() {
        return offsetManager.committed();
    }

    /**
     * 领取下一个任务：从已提交位点之后（committed+1）按 FIFO 扫描，
     * 因此消费重启后不会重复处理已提交任务，也不会丢失未提交任务。
     */
    public Optional<TaskRecord> claimNext(Instant now) {
        return store.withLock(() -> {
            Optional<TaskRecord> claimed = store.claimNext(now, properties.getDeliveryTimeoutMillis());
            claimed.ifPresent(r -> {
                if (r.getId() <= offsetManager.committed()) {
                    // 理论不可达：终态任务会被 claim 扫描跳过；保留为防御性断言
                    throw new IllegalStateException("claimed task below committed offset: taskId="
                            + r.getId() + ", committed=" + offsetManager.committed());
                }
                log.info("claim: taskId={}, attempt={}, businessKey={}, committedOffset={}",
                        r.getId(), r.getAttempts(), r.getBusinessKey(), offsetManager.committed());
            });
            return claimed;
        });
    }

    /**
     * 成功 ACK：存储终态迁移与位点推进在同一把存储锁内完成，整体原子。
     * 重复 ACK（任务已终态）为空操作，不重复推进位点。
     */
    public void ackSuccess(TaskRecord task, Instant now) {
        store.withLock((Runnable) () -> {
            long taskId = task.getId();
            boolean changed = store.completeSuccess(taskId, now);
            if (changed) {
                advanceOffset(taskId);
                log.info("ack success: taskId={}, committedOffset={}, sideEffectDone={}",
                        taskId, offsetManager.committed(), task.isSideEffectDone());
            } else {
                log.info("ack success ignored (already terminal): taskId={}, status={}, committedOffset={}",
                        taskId, task.getStatus(), offsetManager.committed());
            }
        });
    }

    /**
     * 失败上报：按可解释的重试策略决定 RETRY_WAITING（记录退避到 nextVisibleAt）
     * 或 FAILED_PERMANENT（重试耗尽终态）。终态后位点顺序推进。
     */
    public void ackFailure(TaskRecord task, String errorMessage, Instant now) {
        store.withLock((Runnable) () -> {
            long taskId = task.getId();
            RetryDecision decision = retryPolicy.canRetry(task)
                    ? RetryDecision.retryAfter(retryPolicy.nextBackoff(task))
                    : RetryDecision.noRetry();
            Optional<TaskRecord> updated = store.completeFailure(taskId, errorMessage, decision, now);
            if (decision.retry()) {
                log.info("ack failure: taskId={}, attempt={}, retryAfter={}ms, error={}",
                        taskId, task.getAttempts(), decision.backoff().toMillis(), errorMessage);
            } else {
                updated.ifPresent(r -> {
                    if (r.getStatus() == TaskStatus.FAILED_PERMANENT) {
                        advanceOffset(taskId);
                    }
                });
                log.info("ack failure exhausted: taskId={}, attempts={}, status={}, committedOffset={}, error={}",
                        taskId, task.getAttempts(),
                        updated.map(TaskRecord::getStatus).orElse(null),
                        offsetManager.committed(), errorMessage);
            }
        });
    }

    /**
     * 位点推进：只能提交 committed+1 且任务终态；回退或缺口由 OffsetManager 拒绝。
     */
    private void advanceOffset(long taskId) {
        try {
            offsetManager.commit(taskId, id -> store.isTerminal(id));
        } catch (RuntimeException e) {
            log.error("offset commit failed: taskId={}, committed={}, error={}",
                    taskId, offsetManager.committed(), e.getMessage());
            throw e;
        }
    }

    public QueueStats stats() {
        return new QueueStats(
                store.capacity(),
                store.activeCount(),
                store.countByStatus(TaskStatus.PENDING),
                store.countByStatus(TaskStatus.DELIVERING),
                store.countByStatus(TaskStatus.RETRY_WAITING),
                store.countByStatus(TaskStatus.SUCCEEDED),
                store.countByStatus(TaskStatus.FAILED_PERMANENT),
                store.acceptedTotal(),
                store.rejectedDuplicateTotal(),
                rejectedInvalidTotal,
                store.rejectedCapacityTotal() + rejectedRateTotal,
                offsetManager.committed(),
                runningIndicator.getAsBoolean()
        );
    }

    public QueueProperties properties() {
        return properties;
    }

    public void setRunningIndicator(java.util.function.BooleanSupplier runningIndicator) {
        this.runningIndicator = runningIndicator;
    }

    public Clock clock() {
        return clock;
    }
}
