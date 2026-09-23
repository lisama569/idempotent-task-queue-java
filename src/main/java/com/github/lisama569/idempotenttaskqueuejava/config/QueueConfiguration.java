package com.github.lisama569.idempotenttaskqueuejava.config;

import com.github.lisama569.idempotenttaskqueuejava.consumer.ConsumerWorker;
import com.github.lisama569.idempotenttaskqueuejava.limit.RateLimiter;
import com.github.lisama569.idempotenttaskqueuejava.limit.TokenBucketRateLimiter;
import com.github.lisama569.idempotenttaskqueuejava.queue.DefaultTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import com.github.lisama569.idempotenttaskqueuejava.queue.OffsetManager;
import com.github.lisama569.idempotenttaskqueuejava.queue.TaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.retry.ExponentialBackoffRetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.retry.RetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.store.TaskStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
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

    @Bean
    public TaskStore taskStore(QueueProperties props) {
        return new TaskStore(props.getCapacity());
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
    public OffsetManager offsetManager() {
        return new OffsetManager();
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
