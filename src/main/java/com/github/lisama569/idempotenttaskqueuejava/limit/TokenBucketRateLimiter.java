package com.github.lisama569.idempotenttaskqueuejava.limit;

/**
 * 令牌桶：按 permitsPerSecond 匀速补充令牌，桶容量等于每秒速率（允许 1 秒突发）。
 */
public class TokenBucketRateLimiter implements RateLimiter {
    private final double permitsPerSecond;
    private double tokens;
    private long lastRefillMillis;

    public TokenBucketRateLimiter(double permitsPerSecond) {
        this.permitsPerSecond = permitsPerSecond;
        this.tokens = Math.max(1.0, permitsPerSecond);
        this.lastRefillMillis = System.currentTimeMillis();
    }

    @Override
    public synchronized boolean tryAcquire(long nowMillis) {
        long elapsed = nowMillis - lastRefillMillis;
        if (elapsed > 0) {
            double refill = permitsPerSecond * (elapsed / 1000.0);
            tokens = Math.min(permitsPerSecond, tokens + refill);
            lastRefillMillis = nowMillis;
        }
        if (tokens >= 1.0) {
            tokens -= 1.0;
            return true;
        }
        return false;
    }
}
