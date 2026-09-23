package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.EnqueueResult;
import com.github.lisama569.idempotenttaskqueuejava.domain.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景一：同一 businessKey 并发重复投递。
 * 预期：恰好 1 个被接受，其余全部以 DUPLICATE_BUSINESS_KEY 被拒绝；
 * 最终仅 1 条任务记录，拒绝原因与容量/业务校验可区分。
 */
class ConcurrentEnqueueDedupTest {
    private static final Logger log = LoggerFactory.getLogger(ConcurrentEnqueueDedupTest.class);

    @Test
    void sameBusinessKeyConcurrentEnqueue_acceptsExactlyOnce() throws Exception {
        QueueProperties props = TestQueueFactory.props(100, 1000, 3, 20, 100);
        IdempotentTaskQueue queue = TestQueueFactory.fixedBackoffQueue(props);

        int threads = 32;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threads);
        AtomicInteger accepted = new AtomicInteger();
        AtomicInteger duplicateRejected = new AtomicInteger();
        List<EnqueueResult> results = java.util.Collections.synchronizedList(new ArrayList<>());

        for (int i = 0; i < threads; i++) {
            pool.submit(() -> {
                try {
                    start.await();
                    EnqueueResult r = queue.enqueue("order-1001", "email", "hello");
                    results.add(r);
                    if (r.isAccepted()) {
                        accepted.incrementAndGet();
                    } else if (r.getRejectReason() == RejectReason.DUPLICATE_BUSINESS_KEY) {
                        duplicateRejected.incrementAndGet();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    done.countDown();
                }
            });
        }

        start.countDown();
        assertTrue(done.await(10, TimeUnit.SECONDS), "all enqueues finish in time");
        pool.shutdownNow();

        log.info("[TEST-INPUT] threads={}, businessKey=order-1001", threads);
        results.forEach(r -> log.info("[TEST-RESULT] accepted={}, reason={}, taskId={}, existingTaskId={}, detail={}",
                r.isAccepted(), r.getRejectReason(), r.getTaskId(), r.getExistingTaskId(), r.getDetail()));
        log.info("[TEST-OFFSET] committedOffset={}, acceptedTotal={}, rejectedDuplicate={}",
                queue.committedOffset(), queue.stats().acceptedTotal(), queue.stats().rejectedDuplicateTotal());
        log.info("[TEST-BASIS] 单锁内完成 去重检查+插入 => 至多一个 accepted；DUPLICATE 与 INVALID/CAPACITY 枚举可区分");

        assertEquals(1, accepted.get(), "exactly one concurrent enqueue accepted");
        assertEquals(threads - 1, duplicateRejected.get(), "all others rejected as duplicate");
        assertEquals(1, queue.listTasks().size(), "only one task record exists");
        assertEquals(1, queue.stats().acceptedTotal());
        assertEquals(threads - 1, queue.stats().rejectedDuplicateTotal());

        long acceptedId = results.stream().filter(EnqueueResult::isAccepted)
                .findFirst().orElseThrow().getTaskId();
        results.stream().filter(r -> !r.isAccepted()).forEach(r -> {
            assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, r.getRejectReason());
            assertEquals(acceptedId, r.getExistingTaskId(), "reject points to existing task");
            assertFalse(r.getRejectReason().isCapacityReason(), "duplicate is distinguishable from capacity reject");
        });
    }
}
