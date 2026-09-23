package com.github.lisama569.idempotenttaskqueuejava.domain;

import java.time.Duration;
import java.time.Instant;

/**
 * 单个任务的完整状态记录。所有可变状态仅在 TaskStore 锁内变更。
 */
public final class TaskRecord {
    private final long id;
    private final String businessKey;
    private final String type;
    private final String payload;
    private final Instant enqueuedAt;

    private TaskStatus status = TaskStatus.PENDING;
    private int attempts = 0;
    private int maxAttempts;
    private Instant nextVisibleAt;
    private Instant lastFailureAt;
    private String lastErrorMessage;
    private boolean sideEffectDone = false;
    private int staleDeliveries = 0;
    private long version = 0;

    public TaskRecord(long id, String businessKey, String type, String payload,
                      int maxAttempts, Instant now) {
        this.id = id;
        this.businessKey = businessKey;
        this.type = type;
        this.payload = payload;
        this.maxAttempts = maxAttempts;
        this.enqueuedAt = now;
        this.nextVisibleAt = now;
    }

    public long getId() { return id; }
    public String getBusinessKey() { return businessKey; }
    public String getType() { return type; }
    public String getPayload() { return payload; }
    public Instant getEnqueuedAt() { return enqueuedAt; }
    public TaskStatus getStatus() { return status; }
    public int getAttempts() { return attempts; }
    public int getMaxAttempts() { return maxAttempts; }
    public Instant getNextVisibleAt() { return nextVisibleAt; }
    public Instant getLastFailureAt() { return lastFailureAt; }
    public String getLastErrorMessage() { return lastErrorMessage; }
    public boolean isSideEffectDone() { return sideEffectDone; }
    public int getStaleDeliveries() { return staleDeliveries; }
    public long getVersion() { return version; }

    /** 置为投递中并累加尝试次数；nextVisibleAt 用作本次投递的僵死截止线。返回当前是第几次尝试（从 1 开始）。 */
    public int markDelivering(Instant deliveryDeadline) {
        this.attempts++;
        this.status = TaskStatus.DELIVERING;
        this.nextVisibleAt = deliveryDeadline;
        this.version++;
        return this.attempts;
    }

    /** 调度一次重试：进入等待，直到 nextVisibleAt。 */
    public void markRetryWaiting(Instant nextVisibleAt, String errorMessage, Instant failedAt) {
        this.status = TaskStatus.RETRY_WAITING;
        this.nextVisibleAt = nextVisibleAt;
        this.lastFailureAt = failedAt;
        this.lastErrorMessage = errorMessage;
        this.version++;
    }

    /** 进入成功终态。 */
    public void markSucceeded(Instant now) {
        this.status = TaskStatus.SUCCEEDED;
        this.sideEffectDone = true;
        this.lastErrorMessage = null;
        this.version++;
    }

    /** 进入永久失败终态（重试耗尽）。 */
    public void markFailedPermanent(String errorMessage, Instant failedAt) {
        this.status = TaskStatus.FAILED_PERMANENT;
        this.lastFailureAt = failedAt;
        this.lastErrorMessage = errorMessage;
        this.version++;
    }

    /** 标记业务副作用已执行（幂等守卫依据）。 */
    public void markSideEffectDone() {
        this.sideEffectDone = true;
    }

    /** 僵死/重复投递被回收重新调度时计数，便于解释重复消费的判定。 */
    public int countStaleDelivery() {
        this.staleDeliveries++;
        return this.staleDeliveries;
    }
}
