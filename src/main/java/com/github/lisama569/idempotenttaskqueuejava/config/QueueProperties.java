package com.github.lisama569.idempotenttaskqueuejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 队列可配置参数（application.properties 中以 idempotent.queue.* 覆盖）。
 */
@ConfigurationProperties(prefix = "idempotent.queue")
public class QueueProperties {
    private int capacity = 1000;
    private double rateLimitPerSecond = 100.0;
    private int maxAttempts = 3;
    private long retryBackoffMillis = 50;
    private boolean autoStartWorker = true;
    private long deliveryTimeoutMillis = 5000;

    public int getCapacity() { return capacity; }
    public void setCapacity(int capacity) { this.capacity = capacity; }
    public double getRateLimitPerSecond() { return rateLimitPerSecond; }
    public void setRateLimitPerSecond(double rateLimitPerSecond) { this.rateLimitPerSecond = rateLimitPerSecond; }
    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }
    public long getRetryBackoffMillis() { return retryBackoffMillis; }
    public void setRetryBackoffMillis(long retryBackoffMillis) { this.retryBackoffMillis = retryBackoffMillis; }
    public boolean isAutoStartWorker() { return autoStartWorker; }
    public void setAutoStartWorker(boolean autoStartWorker) { this.autoStartWorker = autoStartWorker; }
    public long getDeliveryTimeoutMillis() { return deliveryTimeoutMillis; }
    public void setDeliveryTimeoutMillis(long deliveryTimeoutMillis) { this.deliveryTimeoutMillis = deliveryTimeoutMillis; }
}
