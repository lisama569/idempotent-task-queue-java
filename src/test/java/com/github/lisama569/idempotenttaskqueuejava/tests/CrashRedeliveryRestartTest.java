package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.consumer.ConsumerWorker;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景六：
 * A) 副作用已执行但 ACK 前“进程崩溃”：任务停留在 DELIVERING、位点不前进；
 *    超过投递超时后被重新领取，第二次执行必须跳过副作用；随后 ACK 成功，位点推进一次。
 * B) 消费进程重启：已完成任务不重复，未完成任务在重投/恢复后继续处理，无丢失。
 */
class CrashRedeliveryRestartTest {
    private static final Logger log = LoggerFactory.getLogger(CrashRedeliveryRestartTest.class);

    @Test
    void crashBeforeAck_redelivers_withoutDuplicatingSideEffect() throws Exception {
        QueueProperties props = TestQueueFactory.props(100, 1000, 3, 20, 80);
        IdempotentTaskQueue queue = TestQueueFactory.fixedBackoffQueue(props);
        TestHandlers.CountingHandler handler = new TestHandlers.CountingHandler();
        ConsumerWorker worker = TestQueueFactory.worker(queue, handler);

        long id = queue.enqueue("crash-1", "t", null).getTaskId();
        worker.simulateCrashBeforeAck(id);
        worker.start();

        // 等待“崩溃”发生（轮询，立即检查，保证尚在投递超时窗口内）
        waitUntil(() -> !worker.isRunning(), 1000);
        assertEquals(TaskStatus.DELIVERING, queue.getTask(id).status(), "task stuck in DELIVERING");
        assertEquals(0, queue.committedOffset(), "offset not advanced without ack");
        assertEquals(1, handler.sideEffects.get(), "side-effect executed exactly once before crash");
        log.info("[TEST-OFFSET] after crash: taskId={}, status=DELIVERING, committed=0, sideEffects=1", id);

        // 未到投递超时：不会重投
        assertTrue(queue.claimNext(Instant.now()).isEmpty(), "within delivery timeout, no redelivery");

        // 超过投递超时：僵死任务被重新领取（同一任务ID，attempts 增加）
        Thread.sleep(props.getDeliveryTimeoutMillis() + 20);
        TaskRecord redelivered = queue.claimNext(Instant.now()).orElseThrow(
                () -> new AssertionError("stale delivery must be reclaimable after timeout"));
        assertEquals(id, redelivered.getId());
        assertEquals(2, redelivered.getAttempts(), "redelivery increments attempts");
        assertEquals(1, redelivered.getStaleDeliveries(), "stale delivery counted for explainability");
        log.info("[TEST-RESULT] redelivered same taskId={}, attempt=2, staleCount=1 (判定依据: DELIVERING 超过 {}ms)",
                id, props.getDeliveryTimeoutMillis());

        // 模拟“重启后的新进程”处理重投递：幂等守卫跳过副作用
        ConsumerWorker restarted = TestQueueFactory.worker(queue, handler);
        restarted.processOnce(redelivered);

        assertEquals(TaskStatus.SUCCEEDED, queue.getTask(id).status());
        assertEquals(1, handler.sideEffects.get(), "side-effect still only once after redelivery");
        assertEquals(2, handler.invocations.get(), "handler entered twice (at-least-once), guarded");
        assertEquals(1, queue.committedOffset(), "offset advances exactly once");
        log.info("[TEST-BASIS] at-least-once 投递 + sideEffectDone 幂等守卫 => 结果确定、副作用不重复");
    }

    @Test
    void consumerRestart_resumesFromCommittedOffset_noLossNoDuplicate() {
        QueueProperties props = TestQueueFactory.props(100, 1000, 3, 20, 60_000);
        IdempotentTaskQueue queue = TestQueueFactory.fixedBackoffQueue(props);
        TestHandlers.CountingHandler handler = new TestHandlers.CountingHandler();

        long id1 = queue.enqueue("rs-1", "t", null).getTaskId();
        long id2 = queue.enqueue("rs-2", "t", null).getTaskId();
        log.info("[TEST-INPUT] enqueued {},{}; simulate two consumer process lifecycles", id1, id2);

        // 第一个消费进程：只处理任务1后“停机”
        ConsumerWorker firstProcess = TestQueueFactory.worker(queue, handler);
        TaskRecord t1 = queue.claimNext(Instant.now()).orElseThrow();
        firstProcess.processOnce(t1);
        assertEquals(1, queue.committedOffset());
        assertEquals(1, handler.sideEffects.get());
        log.info("[TEST-OFFSET] first process stops after task1: committed=1, sideEffects=1");

        // 第二个消费进程重启：claim 从 committed+1 恢复，绝不重放已提交任务
        ConsumerWorker secondProcess = TestQueueFactory.worker(queue, handler);
        Optional<TaskRecord> claimed = queue.claimNext(Instant.now());
        assertTrue(claimed.isPresent());
        assertEquals(id2, claimed.get().getId(), "resumes at committed+1, never replays committed task");
        secondProcess.processOnce(claimed.get());

        assertEquals(2, queue.committedOffset());
        assertEquals(2, handler.sideEffects.get(), "each task's side-effect exactly once");
        assertEquals(TaskStatus.SUCCEEDED, queue.getTask(id1).status());
        assertEquals(TaskStatus.SUCCEEDED, queue.getTask(id2).status());
        assertTrue(queue.claimNext(Instant.now()).isEmpty());
        log.info("[TEST-RESULT] after restart: committed=2, sideEffects=2; task1 不重复、task2 不丢失");
        log.info("[TEST-BASIS] 位点单调持久在 OffsetManager，重启扫描跳过终态任务并从 committed+1 继续");
    }

    private static boolean waitUntil(java.util.function.BooleanSupplier cond, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (cond.getAsBoolean()) {
                return true;
            }
            Thread.sleep(5);
        }
        return cond.getAsBoolean();
    }
}
