package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.consumer.ConsumerWorker;
import com.github.lisama569.idempotenttaskqueuejava.domain.EnqueueResult;
import com.github.lisama569.idempotenttaskqueuejava.domain.QueueStats;
import com.github.lisama569.idempotenttaskqueuejava.domain.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskView;
import com.github.lisama569.idempotenttaskqueuejava.persistence.RecoveryReport;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import com.github.lisama569.idempotenttaskqueuejava.queue.TaskContext;
import com.github.lisama569.idempotenttaskqueuejava.queue.TaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.tests.TestQueueFactory.PersistentQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 完整重启（同一存储目录、全新实例）下的持久化与恢复验证。
 * 日志前缀：[TEST-INPUT] 输入 / [TEST-RESTART] 重启前后位点与状态 /
 * [TEST-RESULT] 断言结果 / [TEST-BASIS] 判定依据。
 */
class PersistenceRestartTest {
    private static final Logger log = LoggerFactory.getLogger(PersistenceRestartTest.class);

    @TempDir
    Path tempDir;

    /** 跨“进程”共享的副作用记录：真实重启后也不允许重复执行。 */
    static final class RecordingHandler implements TaskHandler {
        private final List<Long> sideEffectOrder;
        private final Set<Long> failOnceBeforeSuccess = ConcurrentHashMap.newKeySet();

        RecordingHandler(List<Long> sideEffectOrder) {
            this.sideEffectOrder = sideEffectOrder;
        }

        void failOnce(long taskId) {
            failOnceBeforeSuccess.add(taskId);
        }

        @Override
        public void handle(TaskContext context) {
            long id = context.task().getId();
            if (context.sideEffectDone()) {
                log.info("[TEST-HANDLER] taskId={} side-effect already done, skip (idempotent guard)", id);
                return;
            }
            if (failOnceBeforeSuccess.remove(id)) {
                throw new IllegalStateException("planned failure for taskId=" + id);
            }
            sideEffectOrder.add(id);
            context.markSideEffectDone();
        }
    }

    private static void drain(IdempotentTaskQueue q, TaskHandler handler) {
        ConsumerWorker worker = new ConsumerWorker(q, handler, q.clock(), 1L);
        while (true) {
            Optional<TaskRecord> claimed = q.claimNext(q.clock().instant());
            if (claimed.isEmpty()) {
                return;
            }
            worker.processOnce(claimed.get());
        }
    }

    private QueueProperties props(int capacity) {
        return TestQueueFactory.props(capacity, 1000.0, 3, 30, 60_000);
    }

    @Test
    void fullRestartResumesStateOffsetDedupAndOrder() throws Exception {
        Path dir = tempDir.resolve("case1");
        QueueProperties p = props(10);
        List<Long> sideEffects = Collections.synchronizedList(new ArrayList<>());
        RecordingHandler handler = new RecordingHandler(sideEffects);
        handler.failOnce(3); // task3 第一次消费失败一次 -> RETRY_WAITING

        // ---------- 进程 A ----------
        PersistentQueue a = TestQueueFactory.persistentQueue(p, dir);
        log.info("[TEST-INPUT] enqueue k1..k5, consume 1,2 success, 3 fail-once, leave 4,5 pending");
        assertEquals(0, a.persistence().lastRecovery().journalEventsReplayed(),
                "首次启动：空目录等价于全新队列");
        for (int i = 1; i <= 5; i++) {
            EnqueueResult r = a.queue().enqueue("k" + i, "email", "payload-" + i);
            assertTrue(r.isAccepted());
        }
        drain(a.queue(), handler);
        QueueStats statsBefore = a.queue().stats();
        TaskView task1Before = a.queue().getTask(1);
        long offsetBefore = a.queue().committedOffset();
        log.info("[TEST-RESTART] before: offset={}, pending={}, delivering={}, retryWaiting={}, "
                        + "succeeded={}, failed={}, sideEffects={}",
                offsetBefore, statsBefore.pending(), statsBefore.delivering(),
                statsBefore.retryWaiting(), statsBefore.succeeded(),
                statsBefore.failedPermanent(), sideEffects);
        assertEquals(2, offsetBefore);
        assertEquals(2, statsBefore.succeeded());
        assertEquals(1, statsBefore.retryWaiting());
        assertEquals(2, statsBefore.pending());
        // 模拟 kill -9：不 close，直接丢弃实例（所有写入已 fsync）

        // ---------- 进程 B（同目录全新实例） ----------
        PersistentQueue b = TestQueueFactory.persistentQueue(p, dir);
        RecoveryReport report = b.persistence().lastRecovery();
        QueueStats statsAfter = b.queue().stats();
        log.info("[TEST-RESTART] after: offset={}, pending={}, delivering={}, retryWaiting={}, "
                        + "succeeded={}, failed={}, report={}",
                b.queue().committedOffset(), statsAfter.pending(), statsAfter.delivering(),
                statsAfter.retryWaiting(), statsAfter.succeeded(),
                statsAfter.failedPermanent(), report.summarize());
        report.notes().forEach(n -> log.info("[TEST-RESTART] note: {}", n));

        assertEquals(offsetBefore, b.queue().committedOffset(), "消费位置与重启前一致");
        assertEquals(statsBefore.pending(), statsAfter.pending(), "PENDING 数量一致");
        assertEquals(statsBefore.retryWaiting(), statsAfter.retryWaiting(), "RETRY_WAITING 数量一致");
        assertEquals(statsBefore.succeeded(), statsAfter.succeeded(), "SUCCEEDED 数量一致");
        assertEquals(0, statsAfter.delivering(), "重启时不存在在途投递");
        assertEquals(statsBefore.acceptedTotal(), statsAfter.acceptedTotal(), "累计接受数一致");

        TaskView task1After = b.queue().getTask(1);
        assertEquals(task1Before, task1After, "终态任务查询结果与重启前一致");
        assertEquals(TaskStatus.SUCCEEDED, b.queue().findByBusinessKey("k1").orElseThrow().status());

        EnqueueResult dup = b.queue().enqueue("k1", "email", "payload-1");
        log.info("[TEST-RESULT] re-enqueue k1 after restart: accepted={}, reason={}, existing={}",
                dup.isAccepted(), dup.getRejectReason(), dup.getExistingTaskId());
        assertFalse(dup.isAccepted());
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dup.getRejectReason());
        assertEquals(1L, dup.getExistingTaskId(), "用过的业务键重启后仍拦截重复投递");

        // 退避到期后继续消费：已完成的 1,2 不重跑，未完成的 3,4,5 按 FIFO 接上
        Thread.sleep(60);
        drain(b.queue(), handler);
        log.info("[TEST-RESULT] sideEffectOrder={}, finalOffset={}, stats={}",
                sideEffects, b.queue().committedOffset(), b.queue().stats());
        assertEquals(List.of(1L, 2L, 3L, 4L, 5L), sideEffects,
                "已完成任务不重跑、未完成任务不丢且顺序不乱");
        assertEquals(5, b.queue().committedOffset());
        assertEquals(5, b.queue().stats().succeeded());
        log.info("[TEST-BASIS] 依据：快照/日志重放重建同一状态机；位点=最长终态前缀；"
                + "业务键索引随记录恢复；RETRY_WAITING 的 nextVisibleAt 持久化后续期");
        b.close();
    }

    @Test
    void deliveringTaskIsResetAndRedeliveredAfterKill() {
        Path dir = tempDir.resolve("case2");
        QueueProperties p = props(10);
        List<Long> sideEffects = Collections.synchronizedList(new ArrayList<>());
        RecordingHandler handler = new RecordingHandler(sideEffects);

        // ---------- 进程 A：领取后“死在”处理途中 ----------
        PersistentQueue a = TestQueueFactory.persistentQueue(p, dir);
        a.queue().enqueue("k1", "email", "payload-1");
        Optional<TaskRecord> claimed = a.queue().claimNext(a.queue().clock().instant());
        assertTrue(claimed.isPresent());
        log.info("[TEST-INPUT] task1 claimed (DELIVERING, attempt={}), then process killed before ack",
                claimed.get().getAttempts());
        // 不 ACK、不 close：模拟处理途中被杀

        // ---------- 进程 B ----------
        PersistentQueue b = TestQueueFactory.persistentQueue(p, dir);
        RecoveryReport report = b.persistence().lastRecovery();
        log.info("[TEST-RESTART] report={}", report.summarize());
        report.notes().forEach(n -> log.info("[TEST-RESTART] note: {}", n));
        assertEquals(1, report.deliveringResetCount(), "崩溃时在途的任务被确定性重置");
        TaskView t1 = b.queue().getTask(1);
        assertEquals(TaskStatus.PENDING, t1.status(), "在途任务回到待处理，不丢");
        assertEquals(1, t1.attempts(), "已发生的尝试次数保留");

        drain(b.queue(), handler);
        log.info("[TEST-RESULT] sideEffectOrder={}, offset={}", sideEffects, b.queue().committedOffset());
        assertEquals(List.of(1L), sideEffects, "任务被重新投递且副作用只执行一次");
        assertEquals(1, b.queue().committedOffset());
        log.info("[TEST-BASIS] 依据：恢复时 DELIVERING 无存活投递者，重置为 PENDING 并计入报告");
        b.close();
    }

    @Test
    void crashBetweenTerminalStatusAndOffsetWriteRecoversDeterministically() {
        Path dir = tempDir.resolve("case3");
        QueueProperties p = props(10);
        List<Long> sideEffects = Collections.synchronizedList(new ArrayList<>());
        RecordingHandler handler = new RecordingHandler(sideEffects);

        // ---------- 进程 A：终态已落盘、位点未落盘的崩溃点 ----------
        PersistentQueue a = TestQueueFactory.persistentQueue(p, dir);
        a.queue().enqueue("k1", "email", "p1");
        a.queue().enqueue("k2", "email", "p2");
        a.persistence().simulateCrashBeforeNextOffsetWrite();
        log.info("[TEST-INPUT] task1 成功但位点写入被跳过（模拟状态已写、位点未写时宕机）");
        // 只处理 task1：终态落盘、位点不落盘；task2 保持 PENDING
        ConsumerWorker workerA = new ConsumerWorker(a.queue(), handler, a.queue().clock(), 1L);
        Optional<TaskRecord> claimed = a.queue().claimNext(a.queue().clock().instant());
        assertTrue(claimed.isPresent());
        workerA.processOnce(claimed.get());
        // 注意：此时内存位点=1，但磁盘日志里只有 STATUS(1,SUCCEEDED)，没有 OFFSET(1)

        // ---------- 进程 B ----------
        PersistentQueue b = TestQueueFactory.persistentQueue(p, dir);
        RecoveryReport report = b.persistence().lastRecovery();
        log.info("[TEST-RESTART] report={}", report.summarize());
        report.notes().forEach(n -> log.info("[TEST-RESTART] note: {}", n));
        assertTrue(report.offsetAdvancedOverTerminalPrefix(),
                "恢复应识别位点滞后并推进到最长终态前缀");
        assertEquals(0, report.offsetBeforeFix(), "磁盘位点停留在 0");
        assertEquals(1, b.queue().committedOffset(), "位点确定性推进到 1（task1 已终态）");
        assertEquals(TaskStatus.SUCCEEDED, b.queue().getTask(1).status(), "task1 终态不丢");

        drain(b.queue(), handler);
        log.info("[TEST-RESULT] sideEffectOrder={}, offset={}", sideEffects, b.queue().committedOffset());
        assertEquals(List.of(1L, 2L), sideEffects, "task1 不重跑，task2 接上");
        assertEquals(2, b.queue().committedOffset());
        log.info("[TEST-BASIS] 依据：同一任务先写终态再写位点，崩溃只可能造成位点滞后；"
                + "恢复把位点推进到最长终态前缀，等价于补做未落盘的位点提交");
        b.close();
    }

    @Test
    void tornJournalTailIsTruncatedAndStateStaysConsistent() throws Exception {
        Path dir = tempDir.resolve("case4");
        QueueProperties p = props(10);
        List<Long> sideEffects = Collections.synchronizedList(new ArrayList<>());
        RecordingHandler handler = new RecordingHandler(sideEffects);

        // ---------- 进程 A：正常写入后干净关闭 ----------
        PersistentQueue a = TestQueueFactory.persistentQueue(p, dir);
        a.queue().enqueue("k1", "email", "p1");
        a.queue().enqueue("k2", "email", "p2");
        a.queue().enqueue("k3", "email", "p3");
        drain(a.queue(), handler); // task1 成功？不——handler 全部成功，三个都会被消费
        long offsetA = a.queue().committedOffset();
        long journalSizeBefore = a.persistence().journalSizeBytes();
        a.close();
        log.info("[TEST-INPUT] 3 tasks all succeeded, offset={}, journalBytes={}", offsetA, journalSizeBefore);

        // ---------- 模拟“写到一半被杀”：在日志尾部拼接半行撕裂数据 ----------
        byte[] torn = "{\"crc\":999,\"type\":\"STATUS\",\"data\":{\"id\":3,\"stat".getBytes();
        java.nio.file.Files.write(dir.resolve(
                com.github.lisama569.idempotenttaskqueuejava.persistence.PersistenceManager.JOURNAL_FILE),
                torn, java.nio.file.StandardOpenOption.APPEND);
        log.info("[TEST-INPUT] appended {} torn bytes to journal tail (simulating torn write)", torn.length);

        // ---------- 进程 B ----------
        PersistentQueue b = TestQueueFactory.persistentQueue(p, dir);
        RecoveryReport report = b.persistence().lastRecovery();
        log.info("[TEST-RESTART] report={}", report.summarize());
        report.notes().forEach(n -> log.info("[TEST-RESTART] note: {}", n));
        assertEquals(torn.length, report.tornTailBytesTruncated(), "撕裂尾部被识别并截断");
        assertEquals(offsetA, b.queue().committedOffset(), "截断后位点与崩溃前一致");
        assertEquals(3, b.queue().stats().succeeded(), "已落盘的完整记录不受影响");
        assertEquals(journalSizeBefore, b.persistence().journalSizeBytes(),
                "截断落实回文件，下次启动看到干净尾部");

        // 截断后继续正常工作：新任务可投递可消费
        b.queue().enqueue("k4", "email", "p4");
        drain(b.queue(), handler);
        assertEquals(List.of(1L, 2L, 3L, 4L), sideEffects);
        assertEquals(4, b.queue().committedOffset());
        log.info("[TEST-RESULT] sideEffectOrder={}, offset={}", sideEffects, b.queue().committedOffset());
        log.info("[TEST-BASIS] 依据：每行带 CRC，解析/校验失败的尾部视为未完成的写入，"
                + "截断到最后一条完整记录；已 fsync 的前缀不受影响");
        b.close();
    }

    @Test
    void contradictoryPersistedStateRefusesToStart() {
        Path dir = tempDir.resolve("case5");
        QueueProperties p = props(10);
        List<Long> sideEffects = Collections.synchronizedList(new ArrayList<>());
        RecordingHandler handler = new RecordingHandler(sideEffects);

        // ---------- 进程 A：task1 成功（位点=1），task2 仍 PENDING ----------
        PersistentQueue a = TestQueueFactory.persistentQueue(p, dir);
        a.queue().enqueue("k1", "email", "p1");
        a.queue().enqueue("k2", "email", "p2");
        ConsumerWorker workerA = new ConsumerWorker(a.queue(), handler, a.queue().clock(), 1L);
        workerA.processOnce(a.queue().claimNext(a.queue().clock().instant()).orElseThrow());
        a.close();

        // ---------- 人为制造矛盾：位点越过未终态的 task2 ----------
        var journal = new com.github.lisama569.idempotenttaskqueuejava.persistence.Journal(
                dir.resolve(com.github.lisama569.idempotenttaskqueuejava.persistence.PersistenceManager.JOURNAL_FILE));
        journal.append(new com.github.lisama569.idempotenttaskqueuejava.persistence.JournalEvent.OffsetCommitted(2));
        journal.close();
        log.info("[TEST-INPUT] forged OFFSET(2) while task2 is still PENDING (contradiction)");

        // ---------- 进程 B：必须拒绝启动并说明卡点 ----------
        var ex = assertThrows(
                com.github.lisama569.idempotenttaskqueuejava.persistence.RecoveryException.class,
                () -> TestQueueFactory.persistentQueue(p, dir));
        log.info("[TEST-RESULT] startup refused: {}", ex.getMessage());
        assertTrue(ex.getMessage().contains("task 2"), "错误信息指出卡住的任务");
        assertTrue(ex.getMessage().contains("refusing to start"), "错误信息说明拒绝启动");
        log.info("[TEST-BASIS] 依据：位点=2 要求 id<=2 的任务全部终态，task2 为 PENDING，"
                + "自相矛盾不能悄悄糊过去");
    }

    @Test
    void capacityAndDedupSemanticsSurviveRestart() {
        Path dir = tempDir.resolve("case6");
        QueueProperties p = props(3); // 容量=3
        List<Long> sideEffects = Collections.synchronizedList(new ArrayList<>());
        RecordingHandler handler = new RecordingHandler(sideEffects);

        // ---------- 进程 A ----------
        PersistentQueue a = TestQueueFactory.persistentQueue(p, dir);
        a.queue().enqueue("k1", "t", "p1");
        a.queue().enqueue("k2", "t", "p2");
        a.queue().enqueue("k3", "t", "p3");
        EnqueueResult full = a.queue().enqueue("k4", "t", "p4");
        assertEquals(RejectReason.QUEUE_FULL, full.getRejectReason());
        EnqueueResult dupA = a.queue().enqueue("k1", "t", "p1");
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupA.getRejectReason());
        // 完成 task1 释放一个容量
        ConsumerWorker workerA = new ConsumerWorker(a.queue(), handler, a.queue().clock(), 1L);
        workerA.processOnce(a.queue().claimNext(a.queue().clock().instant()).orElseThrow());
        QueueStats before = a.queue().stats();
        log.info("[TEST-RESTART] before: active={}/{}, offset={}, dupRejected={}, capRejected={}",
                before.activeCount(), before.capacity(), before.committedOffset(),
                before.rejectedDuplicateTotal(), before.rejectedCapacityTotal());
        assertEquals(2, before.activeCount());
        // 模拟 kill -9

        // ---------- 进程 B ----------
        PersistentQueue b = TestQueueFactory.persistentQueue(p, dir);
        QueueStats after = b.queue().stats();
        log.info("[TEST-RESTART] after: active={}/{}, offset={}, dupRejected={}, capRejected={}",
                after.activeCount(), after.capacity(), after.committedOffset(),
                after.rejectedDuplicateTotal(), after.rejectedCapacityTotal());
        assertEquals(before.activeCount(), after.activeCount(),
                "重启前占容量的任务重启后仍占容量，已结束的不再占");
        assertEquals(before.rejectedDuplicateTotal(), after.rejectedDuplicateTotal());
        assertEquals(before.rejectedCapacityTotal(), after.rejectedCapacityTotal());

        EnqueueResult accepted = b.queue().enqueue("k4", "t", "p4");
        assertTrue(accepted.isAccepted(), "task1 终态释放的容量重启后可用");
        EnqueueResult dupK2 = b.queue().enqueue("k2", "t", "p2");
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupK2.getRejectReason());
        assertEquals(2L, dupK2.getExistingTaskId());
        EnqueueResult dupK1 = b.queue().enqueue("k1", "t", "p1");
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupK1.getRejectReason(),
                "终态业务键重启后仍拦截");
        EnqueueResult fullAgain = b.queue().enqueue("k5", "t", "p5");
        assertEquals(RejectReason.QUEUE_FULL, fullAgain.getRejectReason(),
                "容量口径与重启前一致（k2,k3,k4 占满 3）");
        log.info("[TEST-RESULT] stats={}", b.queue().stats());
        assertEquals(3, b.queue().stats().rejectedDuplicateTotal(), "拒绝计数跨重启累计");
        log.info("[TEST-BASIS] 依据：activeCount 由恢复的非终态记录重新推导；"
                + "业务键索引与拒绝计数随日志/快照恢复");
        b.close();
    }

    @Test
    void compactionPreservesSemanticsAcrossRestart() throws Exception {
        Path dir = tempDir.resolve("case7");
        QueueProperties p = props(10);
        p.getPersistence().setCompactThresholdBytes(600); // 小阈值迫使运行中自动收拢
        List<Long> sideEffects = Collections.synchronizedList(new ArrayList<>());
        RecordingHandler handler = new RecordingHandler(sideEffects);
        handler.failOnce(3);

        // ---------- 进程 A：足够多的状态变迁触发自动收拢 ----------
        PersistentQueue a = TestQueueFactory.persistentQueue(p, dir);
        for (int i = 1; i <= 4; i++) {
            a.queue().enqueue("k" + i, "email", "payload-" + i);
        }
        drain(a.queue(), handler); // 1,2 成功；3 失败一次进 RETRY_WAITING；4 被堵在队首之后
        EnqueueResult dupBeforeCompact = a.queue().enqueue("k1", "email", "payload-1");
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupBeforeCompact.getRejectReason());
        a.persistence().compactNow(); // 手动再收拢一次
        long journalAfterCompact = a.persistence().journalSizeBytes();
        assertTrue(java.nio.file.Files.exists(dir.resolve("snapshot.jsonl")), "收拢产生快照");
        assertTrue(journalAfterCompact < 600, "收拢后日志被重置: " + journalAfterCompact);
        // 收拢后语义不变：重复投递仍被拦截、失败任务仍可重试
        EnqueueResult dupAfterCompact = a.queue().enqueue("k2", "email", "payload-2");
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupAfterCompact.getRejectReason());
        Thread.sleep(60);
        drain(a.queue(), handler); // task3 重试成功、task4 成功
        QueueStats before = a.queue().stats();
        log.info("[TEST-RESTART] before: offset={}, stats={}", before.committedOffset(), before);
        assertEquals(4, before.committedOffset());
        // 模拟 kill -9

        // ---------- 进程 B：从“快照 + 空日志”恢复 ----------
        PersistentQueue b = TestQueueFactory.persistentQueue(p, dir);
        RecoveryReport report = b.persistence().lastRecovery();
        log.info("[TEST-RESTART] report={}", report.summarize());
        report.notes().forEach(n -> log.info("[TEST-RESTART] note: {}", n));
        assertTrue(report.snapshotUsed(), "重启使用了收拢后的快照");
        QueueStats after = b.queue().stats();
        assertEquals(before.committedOffset(), after.committedOffset());
        assertEquals(before.succeeded(), after.succeeded());
        assertEquals(before.acceptedTotal(), after.acceptedTotal());
        assertEquals(before.rejectedDuplicateTotal(), after.rejectedDuplicateTotal(),
                "收拢不丢拒绝计数");
        EnqueueResult dupAfterRestart = b.queue().enqueue("k1", "email", "payload-1");
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupAfterRestart.getRejectReason(),
                "收拢+重启后历史业务键仍拦截重复投递");
        assertEquals(1L, dupAfterRestart.getExistingTaskId());
        log.info("[TEST-RESULT] sideEffectOrder={}, offset={}, stats={}",
                sideEffects, b.queue().committedOffset(), b.queue().stats());
        assertEquals(List.of(1L, 2L, 3L, 4L), sideEffects, "收拢前后无副作用重放、无丢失");
        log.info("[TEST-BASIS] 依据：收拢只折叠“已被覆盖的历史变迁”，任务记录/业务键/位点/计数器"
                + "全量进入快照；事件应用幂等，收拢中途崩溃重放旧日志不产生矛盾");
        b.close();
    }
}
