package com.github.lisama569.idempotenttaskqueuejava.config;

import com.github.lisama569.idempotenttaskqueuejava.consumer.ConsumerWorker;
import com.github.lisama569.idempotenttaskqueuejava.limit.RateLimiter;
import com.github.lisama569.idempotenttaskqueuejava.limit.TokenBucketRateLimiter;
import com.github.lisama569.idempotenttaskqueuejava.persistence.QueuePersistence;
import com.github.lisama569.idempotenttaskqueuejava.queue.DefaultTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import com.github.lisama569.idempotenttaskqueuejava.queue.OffsetManager;
import com.github.lisama569.idempotenttaskqueuejava.queue.TaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.retry.ExponentialBackoffRetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.retry.RetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.store.TaskStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Optional;

/** 队列组件装配。 */
@Configuration
@EnableConfigurationProperties(QueueProperties.class)
public class QueueConfiguration {

    @Bean
    public Clock queueClock() {
        return Clock.systemUTC();
    }

    /**
     * 本地持久化（可选）：配置 idempotent.queue.persistence-dir 后启用。
     * Bean 创建即执行恢复；恢复发现矛盾状态会抛 RecoveryException，应用拒绝启动。
     */
    @Bean
    @ConditionalOnProperty(prefix = "idempotent.queue", name = "persistence-dir")
    public QueuePersistence queuePersistence(QueueProperties props) {
        return QueuePersistence.open(Path.of(props.getPersistenceDir()));
    }

    @Bean
    public TaskStore taskStore(QueueProperties props, Optional<QueuePersistence> persistence) {
        TaskStore store = new TaskStore(props.getCapacity());
        persistence.ifPresent(p -> {
            QueuePersistence.RecoveredState rs = p.recoveredState();
            store.restoreState(rs.tasks(), rs.sequence(), rs.acceptedTotal(),
                    rs.rejectedDuplicateTotal(), rs.rejectedCapacityTotal());
        });
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
    public OffsetManager offsetManager(Optional<QueuePersistence> persistence) {
        OffsetManager om = new OffsetManager();
        persistence.ifPresent(p -> om.restoreCommitted(p.recoveredState().committedOffset()));
        return om;
    }

    @Bean
    public TaskHandler taskHandler() {
        return new DefaultTaskHandler();
    }

    @Bean
    public IdempotentTaskQueue idempotentTaskQueue(TaskStore store, RateLimiter rateLimiter,
                                                   RetryPolicy retryPolicy, OffsetManager offsetManager,
                                                   QueueProperties props, Clock clock,
                                                   Optional<QueuePersistence> persistence) {
        return new IdempotentTaskQueue(store, rateLimiter, retryPolicy, offsetManager, props, clock,
                persistence.orElse(null));
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
