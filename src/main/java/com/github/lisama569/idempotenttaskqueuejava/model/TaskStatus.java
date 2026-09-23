package com.github.lisama569.idempotenttaskqueuejava.model;

/**
 * 任务生命周期状态。
 *
 * <p>状态迁移：
 * <pre>
 *   PENDING --dispatch--> RUNNING --success--> SUCCEEDED
 *                            |--fail(可重试)--> RETRY_WAIT --到期--> PENDING
 *                            |--fail(耗尽)--> FAILED
 * </pre>
 * SUCCEEDED / FAILED 为终态，不可再迁移；消费位点只能在连续前缀的任务到达终态后推进。
 */
public enum TaskStatus {
    PENDING,
    RETRY_WAIT,
    RUNNING,
    SUCCEEDED,
    FAILED
}
