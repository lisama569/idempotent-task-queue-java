package com.github.lisama569.idempotenttaskqueuejava.consumer;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.queue.DefaultTaskContext;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import com.github.lisama569.idempotenttaskqueuejava.queue.TaskContext;
import com.github.lisama569.idempotenttaskqueuejava.queue.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单分区单线程消费 worker：严格 FIFO 顺序消费。
 *
 * <p>处理契约（at-least-once + 幂等）：</p>
 * <ol>
 *   <li>从队列 claim 一个 DELIVERING 任务（崩溃重启后会领到已超时的同一任务）；</li>
 *   <li>执行业务 handler；副作用由 handler 内的幂等标记保证只生效一次；</li>
 *   <li>成功后 ACK：终态迁移与位点推进原子完成；失败则上报，按策略重试或进终态。</li>
 * </ol>
 * <p>{@link #simulateCrashBeforeAck} 可模拟“副作用已执行但 ACK 前进程崩溃”：
 * worker 线程被中断、任务停留在 DELIVERING，超过投递超时后被重新领取，
 * 第二次执行必须跳过副作用——以此在本地确定性地验证重复消费不产生重复副作用。</p>
 */
public class ConsumerWorker {
    private static final Logger log = LoggerFactory.getLogger(ConsumerWorker.class);

    private final IdempotentTaskQueue queue;
    private final TaskHandler handler;
    private final Clock clock;
    private final long pollIdleMillis;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile Thread thread;
    /** 当且仅当任务ID在此集合中且副作用尚未标记完成后的 ACK 阶段，模拟一次崩溃。 */
    private final java.util.Set<Long> crashBeforeAckOnce =
            java.util.Collections.synchronizedSet(new java.util.HashSet<>());

    public ConsumerWorker(IdempotentTaskQueue queue, TaskHandler handler, Clock clock,
                          long pollIdleMillis) {
        this.queue = queue;
        this.handler = handler;
        this.clock = clock;
        this.pollIdleMillis = pollIdleMillis;
    }

    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        Thread t = new Thread(this::runLoop, "task-consumer");
        t.setDaemon(true);
        t.start();
        thread = t;
        log.info("consumer worker started, committedOffset={}", queue.committedOffset());
    }

    public void stop() {
        running.set(false);
        Thread t = thread;
        if (t != null) {
            t.interrupt();
            try {
                t.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        log.info("consumer worker stopped, committedOffset={}", queue.committedOffset());
    }

    public boolean isRunning() {
        return running.get();
    }

    /** 安排一次“ACK 前崩溃”模拟（仅对该任务生效一次）。 */
    public void simulateCrashBeforeAck(long taskId) {
        crashBeforeAckOnce.add(taskId);
    }

    private void runLoop() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                Optional<TaskRecord> claimed = queue.claimNext(clock.instant());
                if (claimed.isPresent()) {
                    processOnce(claimed.get());
                } else {
                    sleep(pollIdleMillis);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (RuntimeException e) {
                log.error("consumer loop error", e);
                sleepQuiet(pollIdleMillis);
            }
        }
    }

    /**
     * 同步处理一个已 claim 的任务（也供测试确定性调用）。
     */
    public void processOnce(TaskRecord claimed) {
        // 重新从存储取最新记录视图（claim 返回即锁内对象，此处直接使用）
        TaskRecord task = claimed;
        long taskId = task.getId();
        int attempt = task.getAttempts();
        TaskContext context = new DefaultTaskContext(task, attempt, queue::markSideEffectDone);
        try {
            handler.handle(context);
            if (crashBeforeAckOnce.remove(taskId)) {
                // 模拟进程崩溃：不 ACK、不推进位点，任务停留在 DELIVERING
                log.warn("SIMULATED CRASH after side-effect before ack: taskId={}, attempt={}, committedOffset={}",
                        taskId, attempt, queue.committedOffset());
                running.set(false);
                Thread.currentThread().interrupt();
                return;
            }
            queue.ackSuccess(task, clock.instant());
        } catch (Exception e) {
            queue.ackFailure(task, String.valueOf(e.getMessage()), clock.instant());
        }
    }

    private void sleep(long millis) throws InterruptedException {
        Thread.sleep(millis);
    }

    private void sleepQuiet(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
}
