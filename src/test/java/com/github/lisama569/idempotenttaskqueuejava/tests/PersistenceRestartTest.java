package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.consumer.ConsumerWorker;
import com.github.lisama569.idempotenttaskqueuejava.domain.QueueStats;
import com.github.lisama569.idempotenttaskqueuejava.domain.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskView;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 完整重启场景：同一本地存储目录上“进程彻底停掉 → 新实例拉起”。
 * 验证：已完成不重跑、未完成不丢且顺序不乱、消费位置一致、
 * 恢复后容量与去重口径一致、状态数量与终态查询与重启前一致。
 */
class PersistenceRestartTest {
    private static final Logger log = LoggerFactory.getLogger(PersistenceRestartTest.class);

    @TempDir
    Path dir;

    @Test
    void fullRestart_completedNotRerun_unfinishedNotLost_orderPreserved() throws Exception {
        QueueProperties props = TestQueueFactory.props(10, 1000, 3, 20, 60);

        // ---- 第一个进程 ----
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        TestHandlers.CountingHandler h1 = new TestHandlers.CountingHandler();
        ConsumerWorker w1 = TestQueueFactory.worker(q1, h1);

        long id1 = q1.enqueue("k1", "t", "p1").getTaskId();
        long id2 = q1.enqueue("k2", "t", "p2").getTaskId();
        long id3 = q1.enqueue("k3", "t", "p3").getTaskId();
        long id4 = q1.enqueue("k4", "t", "p4").getTaskId();
        long id5 = q1.enqueue("k5", "t", "p5").getTaskId();
        var dup = q1.enqueue("k1", "t", "again");
        assertFalse(dup.isAccepted());
        log.info("[TEST-INPUT] enqueued 5 tasks ids={},{},{},{},{}; duplicate k1 rejected reason={}",
                id1, id2, id3, id4, id5, dup.getRejectReason());

        // t1 直接成功；t2 先失败一次再成功（覆盖 RETRY_WAITING 持久化）；t3 领到一半“进程死掉”
        w1.processOnce(q1.claimNext(Instant.now()).orElseThrow());
        h1.failTimes(id2, 1);
        w1.processOnce(q1.claimNext(Instant.now()).orElseThrow()); // t2 失败 -> RETRY_WAITING
        Thread.sleep(30); // 等退避到期
        w1.processOnce(q1.claimNext(Instant.now()).orElseThrow()); // t2 重试成功
        TaskRecord t3claimed = q1.claimNext(Instant.now()).orElseThrow(); // t3 DELIVERING 后不 ACK，模拟被杀
        assertEquals(id3, t3claimed.getId());

        QueueStats before = q1.stats();
        List<TaskView> viewsBefore = q1.listTasks();
        log.info("[TEST-OFFSET] before kill: committed={}, pending={}, delivering={}, retryWaiting={}, "
                        + "succeeded={}, acceptedTotal={}, rejectedDuplicate={}",
                before.committedOffset(), before.pending(), before.delivering(), before.retryWaiting(),
                before.succeeded(), before.acceptedTotal(), before.rejectedDuplicateTotal());
        assertEquals(2, before.committedOffset());
        assertEquals(1, before.delivering(), "t3 stuck DELIVERING at kill");
        // “杀掉进程”：直接丢弃实例，不执行任何收尾

        // ---- 第二个进程（同一目录） ----
        IdempotentTaskQueue q2 = TestQueueFactory.persistentQueue(props, dir);
        QueueStats after = q2.stats();
        var recovery = q2.lastRecovery();
        log.info("[TEST-OFFSET] after restart: committed={}, pending={}, delivering={}, succeeded={}, "
                        + "recoveredTasks={}, journalFramesReplayed={}, basis={}",
                after.committedOffset(), after.pending(), after.delivering(), after.succeeded(),
                recovery.tasksRecovered(), recovery.journalRecordsReplayed(), recovery.basis());

        assertEquals(before.committedOffset(), after.committedOffset(), "committed offset survives restart");
        assertEquals(before.pending(), after.pending(), "pending count consistent");
        assertEquals(before.delivering(), after.delivering(), "delivering count consistent");
        assertEquals(before.succeeded(), after.succeeded(), "succeeded count consistent");
        assertEquals(before.acceptedTotal(), after.acceptedTotal(), "acceptedTotal consistent");
        assertEquals(before.rejectedDuplicateTotal(), after.rejectedDuplicateTotal(),
                "duplicate-reject counter consistent");
        // 终态查询结果一致
        for (TaskView v : viewsBefore) {
            TaskView r = q2.getTask(v.id());
            assertEquals(v.status(), r.status(), "status of task " + v.id());
            assertEquals(v.attempts(), r.attempts(), "attempts of task " + v.id());
            assertEquals(v.sideEffectDone(), r.sideEffectDone(), "sideEffectDone of task " + v.id());
        }
        // 业务键查重口径一致：旧键仍拒绝，且指向同一个既有任务
        var dupAfter = q2.enqueue("k1", "t", "again");
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupAfter.getRejectReason());
        assertEquals(id1, dupAfter.getExistingTaskId());
        log.info("[TEST-RESULT] dedup after restart: k1 -> DUPLICATE, existingTaskId={} (same as before)",
                dupAfter.getExistingTaskId());

        // 恢复后继续消费：t3 僵死重投（幂等守卫下副作用不重复），t4/t5 按序跟进
        TestHandlers.CountingHandler h2 = new TestHandlers.CountingHandler();
        ConsumerWorker w2 = TestQueueFactory.worker(q2, h2);
        List<Long> processedOrder = new ArrayList<>();
        Thread.sleep(props.getDeliveryTimeoutMillis() + 30); // 等 t3 超过投递超时
        for (int i = 0; i < 3; i++) {
            final int round = i;
            TaskRecord claimed = q2.claimNext(Instant.now()).orElseThrow(
                    () -> new AssertionError("unfinished task lost after restart, round=" + round));
            processedOrder.add(claimed.getId());
            w2.processOnce(claimed);
        }
        assertEquals(List.of(id3, id4, id5), processedOrder, "FIFO order preserved across restart");
        assertEquals(3, h2.sideEffects.get(), "each remaining task executed exactly once");
        assertEquals(2, h1.sideEffects.get(), "pre-restart tasks not re-executed");
        assertEquals(5, q2.committedOffset(), "offset resumes and advances to the end");
        assertEquals(5, q2.stats().succeeded());
        log.info("[TEST-BASIS] 终态+位点同帧落盘 => 已完成不重跑；DELIVERING 超时重投 + sideEffectDone 守卫"
                + " => 被杀任务不丢且副作用不重复；日志按 ID 顺序重放 => 顺序不乱");
    }

    @Test
    void crashAfterSideEffectBeforeAck_redeliverySkipsSideEffect() throws Exception {
        QueueProperties props = TestQueueFactory.props(10, 1000, 3, 20, 60);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        TestHandlers.CountingHandler h1 = new TestHandlers.CountingHandler();
        ConsumerWorker w1 = TestQueueFactory.worker(q1, h1);

        long id = q1.enqueue("se-1", "t", null).getTaskId();
        w1.simulateCrashBeforeAck(id);
        w1.processOnce(q1.claimNext(Instant.now()).orElseThrow());
        Thread.interrupted(); // 清理崩溃模拟留在测试线程上的中断标记
        assertEquals(1, h1.sideEffects.get(), "side-effect ran before crash");
        assertEquals(TaskStatus.DELIVERING, q1.getTask(id).status());
        assertEquals(0, q1.committedOffset());
        log.info("[TEST-INPUT] side-effect done + journaled, then killed before ack: taskId={}, committed=0", id);

        // 重启：副作用标记必须随恢复保留，重投时跳过副作用
        IdempotentTaskQueue q2 = TestQueueFactory.persistentQueue(props, dir);
        assertTrue(q2.getTask(id).sideEffectDone(), "sideEffectDone must survive restart");
        TestHandlers.CountingHandler h2 = new TestHandlers.CountingHandler();
        ConsumerWorker w2 = TestQueueFactory.worker(q2, h2);
        Thread.sleep(props.getDeliveryTimeoutMillis() + 30);
        TaskRecord redelivered = q2.claimNext(Instant.now()).orElseThrow();
        w2.processOnce(redelivered);

        assertEquals(0, h2.sideEffects.get(), "side-effect NOT re-executed after restart");
        assertEquals(1, h2.invocations.get(), "handler entered (at-least-once) but guarded");
        assertEquals(TaskStatus.SUCCEEDED, q2.getTask(id).status());
        assertEquals(1, q2.committedOffset());
        log.info("[TEST-BASIS] SIDE_EFFECT 先落盘再置内存标记 => 崩溃恢复后重投仍跳过副作用，"
                + "sideEffects(h1)={} + sideEffects(h2)={} == 1", h1.sideEffects.get(), h2.sideEffects.get());
    }

    @Test
    void capacityAndDedupSemanticsConsistentAcrossRestarts() {
        QueueProperties props = TestQueueFactory.props(2, 1000, 3, 20, 60_000);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        q1.enqueue("c1", "t", null);
        q1.enqueue("c2", "t", null);
        var full1 = q1.enqueue("c3", "t", null);
        assertEquals(RejectReason.QUEUE_FULL, full1.getRejectReason());
        log.info("[TEST-INPUT] capacity=2, active=2, c3 rejected QUEUE_FULL; kill without processing");

        // 重启后：未完成任务仍占容量
        IdempotentTaskQueue q2 = TestQueueFactory.persistentQueue(props, dir);
        assertEquals(2, q2.stats().activeCount(), "active tasks still occupy capacity after restart");
        var full2 = q2.enqueue("c3", "t", null);
        assertEquals(RejectReason.QUEUE_FULL, full2.getRejectReason(),
                "capacity accounting consistent after restart");
        assertEquals(2, q2.stats().rejectedCapacityTotal(), "capacity-reject counter survives restart");

        // 完成一个任务后再重启：容量释放，c3 可入队
        TaskRecord claimed = q2.claimNext(Instant.now()).orElseThrow();
        q2.ackSuccess(claimed, Instant.now());
        IdempotentTaskQueue q3 = TestQueueFactory.persistentQueue(props, dir);
        assertEquals(1, q3.stats().activeCount(), "terminal task no longer occupies capacity");
        var accepted = q3.enqueue("c3", "t", null);
        assertTrue(accepted.isAccepted(), "freed capacity usable after restart");
        assertEquals(3, q3.stats().acceptedTotal(), "accepted counter cumulative across restarts");
        // 已完成的业务键仍然拒绝重复投递
        var dupTerminal = q3.enqueue("c1", "t", null);
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupTerminal.getRejectReason());
        log.info("[TEST-BASIS] 容量占用来自恢复后的任务状态计数，去重键随快照/日志永久保留 => 口径一致");
    }
}
