package com.github.lisama569.idempotenttaskqueuejava.store;

import java.time.Duration;

/**
 * RetryPolicy 判定结果（值对象），供 TaskStore 在锁内应用。
 */
public record RetryDecision(boolean retry, Duration backoff) {
    public static RetryDecision noRetry() { return new RetryDecision(false, Duration.ZERO); }
    public static RetryDecision retryAfter(Duration backoff) { return new RetryDecision(true, backoff); }
}
