package com.github.lisama569.idempotenttaskqueuejava.domain;

/**
 * 投递被拒绝的可区分原因。
 */
public enum RejectReason {
    DUPLICATE_BUSINESS_KEY,
    INVALID_TASK,
    QUEUE_FULL,
    RATE_LIMITED;

    /** 容量/速率类拒绝，与业务校验类拒绝可区分。 */
    public boolean isCapacityReason() {
        return this == QUEUE_FULL || this == RATE_LIMITED;
    }
}
