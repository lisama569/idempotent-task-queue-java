package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.consumer.ConsumerWorker;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景七：多线程并发投递（含部分重复键）与单 worker 并发消费。
 * 预期：每个不同业务键恰好 1 个任务成功、副作用恰好 1 次；
 * 最终位点连续推进到“不同键的总数”，无丢失、无重复。
 */
class ConcurrentProduceConsumeTest {
    private static final Logger log = LoggerFactory.getLogger(ConcurrentProduceConsumeTest.class);

    @Test
    void concurrentProduceAndConsume_noLossNoDuplicate() throws Exception {
        QueueProperties props = TestQueueFactory.props(500, 100_000, 3, 10, 60_000);
        IdempotentTaskQueue queue = TestQueueFactory.fixedBackoffQueue(props);
        TestHandlers.CountingHandler handler = new TestHandlers.CountingHandler();
        ConsumerWorker worker = TestQueueFactory.worker(queue, handler);

        int distinctKeys = 80;
        int duplicateCopies = 3; // 每个键共投递 3 次（并发）
        int producers = 12;
        ExecutorService pool = Executors.newFixedThreadPool(producers);
        CountDownLatch start = new CountDownLatch(1);
        AtomicInteger queued = new AtomicInteger();

        worker.start();
        for (int copy = 0; copy < duplicateCopies; copy++) {
            for (int k = 1; k <= distinctKeys; k++) {
                final String key = "bulk-" + k;
                pool.submit(() -> {
                    try {
                        start.await();
                        if (queue.enqueue(key, "bulk", null).isAccepted()) {
                            queued.incrementAndGet();
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
        }
        start.countDown();
        pool.shutdown();
        assertTrue(pool.awaitTermination(10, TimeUnit.SECONDS));

        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline
                && queue.committedOffset() < distinctKeys) {
            Thread.sleep(10);
        }
        worker.stop();

        log.info("[TEST-INPUT] distinctKeys={}, duplicateCopies={}, producers={}",
                distinctKeys, duplicateCopies, producers);
        log.info("[TEST-RESULT] accepted={}, committedOffset={}, sideEffects={}, invocations={}, rejectedDuplicate={}",
                queued.get(), queue.committedOffset(), handler.sideEffects.get(),
                handler.invocations.get(), queue.stats().rejectedDuplicateTotal());
        log.info("[TEST-BASIS] accepted==distinctKeys 且 sideEffects==distinctKeys => 无丢失/无重复副作用；位点连续");

        assertEquals(distinctKeys, queued.get(), "one accepted task per distinct business key");
        assertEquals(distinctKeys, queue.committedOffset(), "offset reaches number of distinct keys (contiguous)");
        assertEquals(distinctKeys, handler.sideEffects.get(), "side-effect exactly once per distinct key");
        long succeeded = queue.listTasks().stream()
                .filter(v -> v.status() == TaskStatus.SUCCEEDED).count();
        assertEquals(distinctKeys, succeeded);
    }
}
