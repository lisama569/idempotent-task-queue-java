package com.github.lisama569.idempotenttaskqueuejava.domain;

/**
 * 任务生命周期状态。
 * PENDING -> DELIVERING -> SUCCEEDED
 *                      \-> RETRY_WAITING -> DELIVERING ... -> FAILED_PERMANENT
 */
public enum TaskStatus {
    PENDING,
    DELIVERING,
    RETRY_WAITING,
    SUCCEEDED,
    FAILED_PERMANENT;

    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED_PERMANENT;
    }
}
