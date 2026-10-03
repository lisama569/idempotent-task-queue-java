package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.consumer.ConsumerWorker;
import com.github.lisama569.idempotenttaskqueuejava.limit.TokenBucketRateLimiter;
import com.github.lisama569.idempotenttaskqueuejava.persistence.QueuePersistence;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import com.github.lisama569.idempotenttaskqueuejava.queue.OffsetManager;
import com.github.lisama569.idempotenttaskqueuejava.retry.FixedIntervalRetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.retry.RetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.store.TaskStore;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.time.ZoneOffset;

/** 测试用队列装配：本地内存、无外部依赖、参数可调。 */
public final class TestQueueFactory {

    private TestQueueFactory() {
    }

    public static QueueProperties props(int capacity, double ratePerSecond, int maxAttempts,
                                        long retryBackoffMillis, long deliveryTimeoutMillis) {
        QueueProperties p = new QueueProperties();
        p.setCapacity(capacity);
        p.setRateLimitPerSecond(ratePerSecond);
        p.setMaxAttempts(maxAttempts);
        p.setRetryBackoffMillis(retryBackoffMillis);
        p.setDeliveryTimeoutMillis(deliveryTimeoutMillis);
        p.setAutoStartWorker(false);
        return p;
    }

    public static IdempotentTaskQueue queue(QueueProperties p, Clock clock, RetryPolicy retryPolicy) {
        TaskStore store = new TaskStore(p.getCapacity());
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(p.getRateLimitPerSecond());
        return new IdempotentTaskQueue(store, limiter, retryPolicy, new OffsetManager(), p,
                clock != null ? clock : Clock.fixed(Clock.systemUTC().instant(), ZoneOffset.UTC));
    }

    public static IdempotentTaskQueue fixedBackoffQueue(QueueProperties p) {
        return queue(p, Clock.systemUTC(), new FixedIntervalRetryPolicy(Duration.ofMillis(p.getRetryBackoffMillis())));
    }

    public static ConsumerWorker worker(IdempotentTaskQueue q, TestHandlers.CountingHandler handler) {
        return new ConsumerWorker(q, handler, q.clock(), 5L);
    }

    /**
     * 在同一目录上创建一个“新进程”实例：打开目录即恢复（快照+日志重放），
     * 用同一个 dir 再次调用即模拟进程彻底重启。恢复失败会抛 RecoveryException。
     */
    public static IdempotentTaskQueue persistentQueue(QueueProperties p, Path dir) {
        QueuePersistence persistence = QueuePersistence.open(dir);
        TaskStore store = new TaskStore(p.getCapacity());
        QueuePersistence.RecoveredState rs = persistence.recoveredState();
        store.restoreState(rs.tasks(), rs.sequence(), rs.acceptedTotal(),
                rs.rejectedDuplicateTotal(), rs.rejectedCapacityTotal());
        OffsetManager om = new OffsetManager();
        om.restoreCommitted(rs.committedOffset());
        return new IdempotentTaskQueue(store, new TokenBucketRateLimiter(p.getRateLimitPerSecond()),
                new FixedIntervalRetryPolicy(Duration.ofMillis(p.getRetryBackoffMillis())), om, p,
                Clock.systemUTC(), persistence);
    }
}
