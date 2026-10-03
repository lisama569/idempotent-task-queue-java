package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.consumer.ConsumerWorker;
import com.github.lisama569.idempotenttaskqueuejava.limit.TokenBucketRateLimiter;
import com.github.lisama569.idempotenttaskqueuejava.persistence.PersistenceManager;
import com.github.lisama569.idempotenttaskqueuejava.persistence.RecoveredState;
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
     * 持久化队列实例（一次“进程生命周期”）：同一目录上销毁旧实例、新建实例即模拟完整重启。
     * 模拟崩溃时直接丢弃引用即可（每次写入均已 fsync，不 close 等价于 kill -9）。
     */
    public record PersistentQueue(IdempotentTaskQueue queue, PersistenceManager persistence,
                                  TaskStore store, OffsetManager offsets) implements AutoCloseable {
        @Override
        public void close() {
            persistence.close();
        }
    }

    public static PersistentQueue persistentQueue(QueueProperties p, Path dir) {
        PersistenceManager pm = new PersistenceManager(dir,
                p.getPersistence().getCompactThresholdBytes());
        RecoveredState rs = pm.recover();
        TaskStore store = TaskStore.restore(p.getCapacity(), rs.records(), rs.sequence(),
                rs.acceptedTotal(), rs.rejectedDuplicateTotal(), rs.rejectedCapacityTotal());
        store.setListener(pm);
        OffsetManager offsets = new OffsetManager(rs.committedOffset());
        offsets.setCommitListener(pm::onOffsetCommitted);
        TokenBucketRateLimiter limiter = new TokenBucketRateLimiter(p.getRateLimitPerSecond());
        IdempotentTaskQueue q = new IdempotentTaskQueue(store, limiter,
                new FixedIntervalRetryPolicy(Duration.ofMillis(p.getRetryBackoffMillis())),
                offsets, p, Clock.systemUTC());
        return new PersistentQueue(q, pm, store, offsets);
    }
}
