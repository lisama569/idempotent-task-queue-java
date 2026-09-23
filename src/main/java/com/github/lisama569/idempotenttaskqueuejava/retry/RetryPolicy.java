package com.github.lisama569.idempotenttaskqueuejava.retry;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import java.time.Duration;

/**
 * 失败重试策略：判定是否还能重试，以及下一次重试的退避时长。
 */
public interface RetryPolicy {
    boolean canRetry(TaskRecord record);
    Duration nextBackoff(TaskRecord record);
}
