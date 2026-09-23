package com.github.lisama569.idempotenttaskqueuejava.retry;

import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;

/**
 * 失败重试策略：决定失败后是否还能重试，以及下一次重试的等待时间。
 */
public interface RetryPolicy {

    /** 是否还有剩余尝试机会（attempts 为本次失败后已尝试的次数）。 */
    boolean canRetry(TaskRecord task);

    /** 距离下次可调度的等待毫秒数。 */
    long backoffMillis(TaskRecord task);
}
