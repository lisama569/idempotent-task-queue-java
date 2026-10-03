package com.github.lisama569.idempotenttaskqueuejava.config;

import com.github.lisama569.idempotenttaskqueuejava.consumer.ConsumerWorker;
import com.github.lisama569.idempotenttaskqueuejava.limit.RateLimiter;
import com.github.lisama569.idempotenttaskqueuejava.limit.TokenBucketRateLimiter;
import com.github.lisama569.idempotenttaskqueuejava.persistence.PersistenceManager;
import com.github.lisama569.idempotenttaskqueuejava.persistence.RecoveredState;
import com.github.lisama569.idempotenttaskqueuejava.queue.DefaultTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import com.github.lisama569.idempotenttaskqueuejava.queue.OffsetManager;
import com.github.lisama569.idempotenttaskqueuejava.queue.TaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.retry.ExponentialBackoffRetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.retry.RetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.store.TaskStore;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;

/** 队列组件装配。 */
@Configuration
@EnableConfigurationProperties(QueueProperties.class)
public class QueueConfiguration {

    @Bean
    public Clock queueClock() {
        return Clock.systemUTC();
    }

    /**
     * 本地持久化（默认开启）：Bean 创建时即完成恢复，
     * 恢复失败（数据自相矛盾）直接抛出导致应用拒绝启动。
     */
    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "idempotent.queue.persistence", name = "enabled",
            havingValue = "true", matchIfMissing = true)
    public PersistenceManager persistenceManager(QueueProperties props) {
        PersistenceManager pm = new PersistenceManager(Path.of(props.getPersistence().getDir()),
                props.getPersistence().getCompactThresholdBytes());
        pm.recover();
        return pm;
    }

    @Bean
    public TaskStore taskStore(QueueProperties props, ObjectProvider<PersistenceManager> pmProvider) {
        PersistenceManager pm = pmProvider.getIfAvailable();
        if (pm == null) {
            return new TaskStore(props.getCapacity());
        }
        RecoveredState rs = pm.recoveredState();
        TaskStore store = TaskStore.restore(props.getCapacity(), rs.records(), rs.sequence(),
                rs.acceptedTotal(), rs.rejectedDuplicateTotal(), rs.rejectedCapacityTotal());
        store.setListener(pm);
        return store;
    }

    @Bean
    public RateLimiter rateLimiter(QueueProperties props) {
        return new TokenBucketRateLimiter(props.getRateLimitPerSecond());
    }

    @Bean
    public RetryPolicy retryPolicy(QueueProperties props) {
        return new ExponentialBackoffRetryPolicy(
                Duration.ofMillis(props.getRetryBackoffMillis()),
                Duration.ofSeconds(5));
    }

    @Bean
    public OffsetManager offsetManager(ObjectProvider<PersistenceManager> pmProvider) {
        PersistenceManager pm = pmProvider.getIfAvailable();
        OffsetManager om = pm == null ? new OffsetManager()
                : new OffsetManager(pm.recoveredState().committedOffset());
        if (pm != null) {
            om.setCommitListener(pm::onOffsetCommitted);
        }
        return om;
    }

    @Bean
    public TaskHandler taskHandler() {
        return new DefaultTaskHandler();
    }

    @Bean
    public IdempotentTaskQueue idempotentTaskQueue(TaskStore store, RateLimiter rateLimiter,
                                                   RetryPolicy retryPolicy, OffsetManager offsetManager,
                                                   QueueProperties props, Clock clock) {
        return new IdempotentTaskQueue(store, rateLimiter, retryPolicy, offsetManager, props, clock);
    }

    @Bean
    public ConsumerWorker consumerWorker(IdempotentTaskQueue queue, TaskHandler handler, Clock clock) {
        ConsumerWorker worker = new ConsumerWorker(queue, handler, clock, 10L);
        queue.setRunningIndicator(worker::isRunning);
        return worker;
    }

    @Bean
    public com.github.lisama569.idempotenttaskqueuejava.consumer.WorkerLifecycle
            workerLifecycle(ConsumerWorker worker, QueueProperties props) {
        return new com.github.lisama569.idempotenttaskqueuejava.consumer.WorkerLifecycle(worker, props);
    }
}
