package com.github.lisama569.idempotenttaskqueuejava.limit;

/**
 * 投递速率限制器（令牌桶）。tryAcquire 必须线程安全。
 */
public interface RateLimiter {
    boolean tryAcquire(long nowMillis);
}
