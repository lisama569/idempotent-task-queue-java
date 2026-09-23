package com.github.lisama569.idempotenttaskqueuejava.retry;

import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;

/**
 * 固定退避重试策略。
 *
 * <p>可解释规则：每次执行失败后，若 attempts &lt; maxAttempts 则可再试一次，
 * 等待固定的 backoffMillis；否则重试耗尽，任务进入 FAILED 终态。
 */
public class FixedBackoffRetryPolicy implements RetryPolicy {

    private final long backoffMillis;

    public FixedBackoffRetryPolicy(long backoffMillis) {
        if (backoffMillis < 0) {
            throw new IllegalArgumentException("backoffMillis 不能为负");
        }
        this.backoffMillis = backoffMillis;
    }

    @Override
    public boolean canRetry(TaskRecord task) {
        // attempts 表示“本次失败之后”累计已尝试次数
        return task.getAttempts() < task.getMaxAttempts();
    }

    @Override
    public long backoffMillis(TaskRecord task) {
        return backoffMillis;
    }
}
