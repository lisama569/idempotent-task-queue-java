package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.EnqueueResult;
import com.github.lisama569.idempotenttaskqueuejava.domain.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景五：容量满拒绝 QUEUE_FULL；速率超限拒绝 RATE_LIMITED；业务字段非法拒绝 INVALID_TASK。
 * 三类原因彼此可区分；容量/速率拒绝不改变存储，已接受的排队任务不受影响。
 */
class CapacityAndRateLimitTest {
    private static final Logger log = LoggerFactory.getLogger(CapacityAndRateLimitTest.class);

    @Test
    void capacityRejectIsDistinguishable_andDoesNotAffectQueuedTasks() {
        QueueProperties props = TestQueueFactory.props(3, 1000, 3, 20, 100);
        IdempotentTaskQueue queue = TestQueueFactory.fixedBackoffQueue(props);

        for (int i = 1; i <= 3; i++) {
            assertTrue(queue.enqueue("cap-" + i, "t", null).isAccepted());
        }
        EnqueueResult rejected = queue.enqueue("cap-overflow", "t", null);
        log.info("[TEST-INPUT] capacity=3, enqueued 3 then one more: accepted={}, reason={}, detail={}",
                rejected.isAccepted(), rejected.getRejectReason(), rejected.getDetail());
        log.info("[TEST-OFFSET] committedOffset={} (reject must not move it), active={}",
                queue.committedOffset(), queue.stats().activeCount());

        assertFalse(rejected.isAccepted());
        assertEquals(RejectReason.QUEUE_FULL, rejected.getRejectReason());
        assertTrue(rejected.getRejectReason().isCapacityReason());
        assertEquals(3, queue.stats().activeCount(), "no slot consumed by rejected task");
        assertEquals(0, queue.committedOffset(), "reject does not touch offset");

        // 与业务校验失败可区分
        EnqueueResult invalid = queue.enqueue("  ", "t", null);
        assertEquals(RejectReason.INVALID_TASK, invalid.getRejectReason());
        assertFalse(invalid.getRejectReason().isCapacityReason());

        // 已排队任务完整无损，仍可按 FIFO 被领取
        long firstId = queue.claimNext(java.time.Instant.now()).orElseThrow().getId();
        assertEquals(1, firstId, "accepted queued tasks remain intact");
        log.info("[TEST-BASIS] QUEUE_FULL/INVALID_TASK 不同枚举；拒绝路径不写存储 => 排队任务不受影响");
    }

    @Test
    void rateLimitRejectIsDistinguishableFromDuplicateAndInvalid() throws Exception {
        QueueProperties props = TestQueueFactory.props(10_000, 2.0, 3, 20, 100);
        IdempotentTaskQueue queue = TestQueueFactory.fixedBackoffQueue(props);

        // 桶初始容量约 2 个令牌：前两个被接受，第三个立刻被速率拒绝
        EnqueueResult r1 = queue.enqueue("rate-1", "t", null);
        EnqueueResult r2 = queue.enqueue("rate-2", "t", null);
        EnqueueResult r3 = queue.enqueue("rate-3", "t", null);

        log.info("[TEST-INPUT] rateLimit=2/s, burst results: r1.accepted={}, r2.accepted={}, r3.reason={}",
                r1.isAccepted(), r2.isAccepted(), r3.getRejectReason());

        assertTrue(r1.isAccepted());
        assertTrue(r2.isAccepted());
        assertFalse(r3.isAccepted());
        assertEquals(RejectReason.RATE_LIMITED, r3.getRejectReason());
        assertTrue(r3.getRejectReason().isCapacityReason());

        // 速率拒绝后等待补充，新任务可以被接受（证明限流器恢复且队列未受污染）
        Thread.sleep(1100);
        EnqueueResult r4 = queue.enqueue("rate-4", "t", null);
        assertTrue(r4.isAccepted(), "token refill restores acceptance");

        // 重复键原因仍然独立可区分
        EnqueueResult dup = queue.enqueue("rate-1", "t", null);
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dup.getRejectReason());
        assertFalse(dup.getRejectReason().isCapacityReason());
        log.info("[TEST-BASIS] RATE_LIMITED / DUPLICATE_BUSINESS_KEY / INVALID_TASK 三类错误码互斥可区分");
    }
}
