package com.github.lisama569.idempotenttaskqueuejava.consumer;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import org.springframework.context.SmartLifecycle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 消费 worker 的应用级生命周期：随 Spring 容器启动/停止。
 * 消费重启（stop 后再 start）时，worker 从已提交位点的下一个任务继续，
 * 已提交的不重复、未提交的不丢失。
 */
public class WorkerLifecycle implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(WorkerLifecycle.class);

    private final ConsumerWorker worker;
    private final QueueProperties properties;
    private volatile boolean started = false;

    public WorkerLifecycle(ConsumerWorker worker, QueueProperties properties) {
        this.worker = worker;
        this.properties = properties;
    }

    @Override
    public void start() {
        if (properties.isAutoStartWorker()) {
            worker.start();
            started = true;
        } else {
            log.info("auto-start worker disabled by idempotent.queue.auto-start-worker=false");
        }
    }

    @Override
    public void stop() {
        if (started) {
            worker.stop();
            started = false;
        }
    }

    @Override
    public boolean isRunning() {
        return started && worker.isRunning();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }
}
