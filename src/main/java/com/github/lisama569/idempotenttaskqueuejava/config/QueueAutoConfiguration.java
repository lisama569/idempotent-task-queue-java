package com.github.lisama569.idempotenttaskqueuejava.config;

import com.github.lisama569.idempotenttaskqueuejava.handler.AlwaysFailTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.FlakyTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.SuccessTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.TaskHandlerRegistry;
import com.github.lisama569.idempotenttaskqueuejava.queue.ConsumerLoop;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import com.github.lisama569.idempotenttaskqueuejava.retry.FixedBackoffRetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.storage.WalStore;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;

/**
 * 队列组件装配：WAL -&gt; 重试策略 -&gt; 处理器注册表 -&gt; 队列核心(启动回放) -&gt; 消费循环。
 */
@Configuration
@EnableConfigurationProperties(QueueProperties.class)
public class QueueAutoConfiguration {

    @Bean
    public TaskHandlerRegistry taskHandlerRegistry() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry();
        registry.register(new SuccessTaskHandler());
        registry.register(new AlwaysFailTaskHandler());
        registry.register(new FlakyTaskHandler());
        return registry;
    }

    @Bean
    public WalStore walStore(QueueProperties props) {
        Path logFile = Path.of(props.getDataDir(), "queue.wal");
        return new WalStore(logFile);
    }

    @Bean
    public IdempotentTaskQueue idempotentTaskQueue(WalStore walStore,
                                                   TaskHandlerRegistry registry,
                                                   QueueProperties props) {
        IdempotentTaskQueue queue = new IdempotentTaskQueue(
                walStore, registry,
                new FixedBackoffRetryPolicy(props.getRetryBackoffMillis()),
                props.getCapacity(),
                props.getRateLimitPerSecond());
        queue.recover();
        return queue;
    }

    @Bean
    public ConsumerLoop consumerLoop(IdempotentTaskQueue queue,
                                     TaskHandlerRegistry registry,
                                     QueueProperties props) {
        ConsumerLoop loop = new ConsumerLoop(queue, registry, props.getPollMillis());
        if (props.isAutoStartConsumer()) {
            loop.start();
        }
        return loop;
    }
}
