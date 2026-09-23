package com.github.lisama569.idempotenttaskqueuejava.retry;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import java.time.Duration;

/** 指数退避重试策略：backoff = base * 2^(attempt-1)，并以固定抖动打散并发重试。 */
public class ExponentialBackoffRetryPolicy implements RetryPolicy {
    private final Duration base;
    private final Duration maxBackoff;

    public ExponentialBackoffRetryPolicy(Duration base, Duration maxBackoff) {
        this.base = base;
        this.maxBackoff = maxBackoff;
    }

    @Override
    public boolean canRetry(TaskRecord record) {
        return record.getAttempts() < record.getMaxAttempts();
    }

    @Override
    public Duration nextBackoff(TaskRecord record) {
        // 第 attempts 次失败后：base * 2^(attempts-1)，上限 maxBackoff
        long baseMillis = base.toMillis();
        int shift = Math.min(record.getAttempts() - 1, 30);
        long computed = baseMillis << shift;
        if (computed < baseMillis || computed > maxBackoff.toMillis()) {
            computed = maxBackoff.toMillis();
        }
        // 固定 20% 抖动（可解释、可复现：基于任务ID与尝试次数），避免并发重试同时唤醒
        long jitter = (computed / 5) * ((int) (Math.abs(record.getId() * 31L + record.getAttempts()) % 5)) / 4;
        return Duration.ofMillis(computed + jitter);
    }
}
