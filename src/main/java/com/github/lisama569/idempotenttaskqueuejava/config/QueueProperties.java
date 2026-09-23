package com.github.lisama569.idempotenttaskqueuejava.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 队列可配置项，前缀 idempotent.queue。
 */
@ConfigurationProperties(prefix = "idempotent.queue")
public class QueueProperties {

    /** WAL 日志文件目录。 */
    private String dataDir = "build/queue-data";
    /** 未完成任务（PENDING/RETRY_WAIT/RUNNING）容量上限。 */
    private int capacity = 100;
    /** 每秒接受投递的最大数量（滑动窗口）。 */
    private int rateLimitPerSecond = 20;
    /** 默认最大尝试次数。 */
    private int defaultMaxAttempts = 3;
    /** 固定重试退避毫秒数。 */
    private long retryBackoffMillis = 200;
    /** 消费者轮询间隔毫秒数。 */
    private long pollMillis = 20;
    /** 是否随应用启动消费者。 */
    private boolean autoStartConsumer = true;

    public String getDataDir() { return dataDir; }
    public void setDataDir(String dataDir) { this.dataDir = dataDir; }

    public int getCapacity() { return capacity; }
    public void setCapacity(int capacity) { this.capacity = capacity; }

    public int getRateLimitPerSecond() { return rateLimitPerSecond; }
    public void setRateLimitPerSecond(int rateLimitPerSecond) { this.rateLimitPerSecond = rateLimitPerSecond; }

    public int getDefaultMaxAttempts() { return defaultMaxAttempts; }
    public void setDefaultMaxAttempts(int defaultMaxAttempts) { this.defaultMaxAttempts = defaultMaxAttempts; }

    public long getRetryBackoffMillis() { return retryBackoffMillis; }
    public void setRetryBackoffMillis(long retryBackoffMillis) { this.retryBackoffMillis = retryBackoffMillis; }

    public long getPollMillis() { return pollMillis; }
    public void setPollMillis(long pollMillis) { this.pollMillis = pollMillis; }

    public boolean isAutoStartConsumer() { return autoStartConsumer; }
    public void setAutoStartConsumer(boolean autoStartConsumer) { this.autoStartConsumer = autoStartConsumer; }
}
