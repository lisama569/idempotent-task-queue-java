package com.github.lisama569.idempotenttaskqueuejava.model;

/**
 * 投递被拒绝时的可区分原因。
 */
public enum RejectReason {
    /** 同一业务键任务已存在（无论处于排队/执行中还是终态），重复投递被幂等拒绝。 */
    DUPLICATE_BUSINESS_KEY,
    /** 队列中未完成任务数已达容量上限，不影响已接受的排队任务。 */
    CAPACITY_EXCEEDED,
    /** 投递速率超过滑动窗口上限。 */
    RATE_LIMITED,
    /** 请求体/业务键等输入校验失败。 */
    VALIDATION_FAILED
}
