package com.github.lisama569.idempotenttaskqueuejava.retry;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import java.time.Duration;

/** 固定间隔重试策略。 */
public class FixedIntervalRetryPolicy implements RetryPolicy {
    private final Duration backoff;

    public FixedIntervalRetryPolicy(Duration backoff) {
        this.backoff = backoff;
    }

    @Override
    public boolean canRetry(TaskRecord record) {
        return record.getAttempts() < record.getMaxAttempts();
    }

    @Override
    public Duration nextBackoff(TaskRecord record) {
        return backoff;
    }
}
