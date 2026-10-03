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
    private Persistence persistence = new Persistence();

    /** 本地持久化与恢复配置。 */
    public static class Persistence {
        /** 是否启用本地持久化（关闭即退回纯内存模式，行为与旧版一致）。 */
        private boolean enabled = true;
        /** 本地存储目录（快照 + 日志）；同一目录重启即恢复。 */
        private String dir = "./queue-data";
        /** 日志超过该字节数即触发历史收拢（快照压缩 + 日志重置）。 */
        private long compactThresholdBytes = 1_048_576L;

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public String getDir() { return dir; }
        public void setDir(String dir) { this.dir = dir; }
        public long getCompactThresholdBytes() { return compactThresholdBytes; }
        public void setCompactThresholdBytes(long compactThresholdBytes) { this.compactThresholdBytes = compactThresholdBytes; }
    }

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
    public Persistence getPersistence() { return persistence; }
    public void setPersistence(Persistence persistence) { this.persistence = persistence; }
}
