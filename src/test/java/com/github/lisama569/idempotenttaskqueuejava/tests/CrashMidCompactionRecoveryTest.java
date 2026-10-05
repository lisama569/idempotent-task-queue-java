package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.QueueStats;
import com.github.lisama569.idempotenttaskqueuejava.domain.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.persistence.RecoveryException;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.zip.CRC32;

import static org.junit.jupiter.api.Assertions.*;

/**
 * “历史清理/收拢进行到一半被强杀”场景：
 * A) 收拢在“快照已原子 rename、日志尚未清空”之间被杀 → 重启后拒绝计数（重复投递/容量）、
 *    各状态数量、消费位点、业务键查重结果与停机前完全一致（重叠事件按版本号/rejectSeq 幂等跳过），
 *    且恢复后新的拒绝、重试、消费、再次收拢全部照常；
 * B) 旧格式（v1 快照 + 无序号 REJECT 帧）无法判定拒绝是否已计入快照 → 明确拒绝启动并说明依据。
 */
class CrashMidCompactionRecoveryTest {
    private static final Logger log = LoggerFactory.getLogger(CrashMidCompactionRecoveryTest.class);

    @TempDir
    Path dir;

    /** 模拟“进程在收拢中途被杀”：钩子抛出即视为进程消失，磁盘上留下 快照已替换+日志未清 的重叠状态。 */
    private static final class SimulatedKillException extends RuntimeException {
        SimulatedKillException() {
            super("simulated kill between snapshot rename and journal truncate");
        }
    }

    @Test
    void killMidCompaction_restartKeepsRejectCountsAndContinues() throws Exception {
        QueueProperties props = TestQueueFactory.props(3, 1000, 3, 20, 60);
        var h1 = new TestHandlers.CountingHandler();

        // ---- 阶段A：制造历史 —— 重复投递被拒、容量已满被拒、成功、退避中 ----
        var holder1 = TestQueueFactory.openPersistent(props, dir);
        IdempotentTaskQueue q1 = holder1.queue();
        q1.enqueue("mc-1", "t", null);
        q1.enqueue("mc-2", "t", null);
        q1.enqueue("mc-3", "t", null); // active=3=capacity
        q1.enqueue("mc-1", "t", null); // DUPLICATE #1
        q1.enqueue("mc-x", "t", null); // QUEUE_FULL #1
        q1.enqueue("mc-2", "t", null); // DUPLICATE #2
        q1.enqueue("mc-y", "t", null); // QUEUE_FULL #2
        processOne(q1, h1);            // task1 SUCCEEDED, committed=1, active=2
        q1.enqueue("mc-4", "t", null); // accepted id=4, active=3
        q1.enqueue("mc-z", "t", null); // QUEUE_FULL #3
        h1.failTimes(2, 1);
        processOne(q1, h1);            // task2 失败一次 -> RETRY_WAITING

        QueueStats before = q1.stats();
        long journalBefore = Files.size(dir.resolve("journal.log"));
        log.info("[TEST-INPUT] pre-kill: accepted={}, rejectedDup={}, rejectedCap={}, committed={}, "
                        + "pending={}, retryWaiting={}, succeeded={}, journalBytes={}",
                before.acceptedTotal(), before.rejectedDuplicateTotal(), before.rejectedCapacityTotal(),
                before.committedOffset(), before.pending(), before.retryWaiting(), before.succeeded(),
                journalBefore);
        assertEquals(4, before.acceptedTotal());
        assertEquals(2, before.rejectedDuplicateTotal());
        assertEquals(3, before.rejectedCapacityTotal());
        assertEquals(1, before.committedOffset());
        assertEquals(TaskStatus.RETRY_WAITING, q1.getTask(2).status());

        // ---- 阶段B：收拢在 rename 与清日志之间被强杀 ----
        holder1.persistence().setCompactionCrashHook(() -> {
            throw new SimulatedKillException();
        });
        SimulatedKillException kill = assertThrows(SimulatedKillException.class, q1::compact,
                "compaction must abort at the simulated kill point");
        log.info("[TEST-INPUT] killed mid-compaction: {}", kill.getMessage());
        long journalAfterKill = Files.size(dir.resolve("journal.log"));
        assertTrue(Files.exists(dir.resolve("snapshot.dat")), "snapshot renamed before kill");
        assertEquals(journalBefore, journalAfterKill,
                "journal not truncated: snapshot and journal overlap on disk");
        log.info("[TEST-INPUT] on-disk overlap state: snapshot.dat exists, journalBytes={} (untruncated)",
                journalAfterKill);
        holder1.persistence().close(); // 模拟进程退出释放文件句柄

        // ---- 阶段C：同一目录重启 —— 计数/状态/位点/查重必须与停机前完全一致 ----
        var holder2 = TestQueueFactory.openPersistent(props, dir);
        IdempotentTaskQueue q2 = holder2.queue();
        var recovery = q2.lastRecovery();
        QueueStats after = q2.stats();
        log.info("[TEST-OFFSET] restart: snapshotLoaded={}, framesReplayed={}, eventsSkipped={}, "
                        + "committed={}, basis={}",
                recovery.snapshotLoaded(), recovery.journalRecordsReplayed(),
                recovery.journalEventsSkipped(), recovery.committedOffset(), recovery.basis());
        log.info("[TEST-RESULT] post-restart: accepted={}, rejectedDup={}, rejectedCap={}, "
                        + "pending={}, retryWaiting={}, succeeded={}",
                after.acceptedTotal(), after.rejectedDuplicateTotal(), after.rejectedCapacityTotal(),
                after.pending(), after.retryWaiting(), after.succeeded());
        assertTrue(recovery.snapshotLoaded());
        assertTrue(recovery.journalEventsSkipped() > 0,
                "overlapped events must be detected and skipped, not re-applied");
        assertEquals(before.rejectedDuplicateTotal(), after.rejectedDuplicateTotal(),
                "duplicate-reject count must not double after mid-compaction kill");
        assertEquals(before.rejectedCapacityTotal(), after.rejectedCapacityTotal(),
                "capacity-reject count must not double after mid-compaction kill");
        assertEquals(before.acceptedTotal(), after.acceptedTotal());
        assertEquals(before.committedOffset(), after.committedOffset());
        assertEquals(before.pending(), after.pending());
        assertEquals(before.retryWaiting(), after.retryWaiting());
        assertEquals(before.succeeded(), after.succeeded());
        // 业务键查重口径一致
        for (long id = 1; id <= 4; id++) {
            long expectedId = id;
            assertEquals(expectedId, q2.findByBusinessKey("mc-" + id).orElseThrow().id(),
                    "businessKey mc-" + id + " must still resolve to task " + id);
        }
        assertTrue(q2.findByBusinessKey("mc-never").isEmpty());
        assertConsistent(q2);

        // ---- 阶段D：恢复后新的拒绝/重试/消费照常 ----
        var h2 = new TestHandlers.CountingHandler();
        var dup = q2.enqueue("mc-1", "t", null);
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dup.getRejectReason());
        assertEquals(1, dup.getExistingTaskId());
        assertEquals(before.rejectedDuplicateTotal() + 1, q2.stats().rejectedDuplicateTotal(),
                "new duplicate reject increments exactly once");
        var full = q2.enqueue("mc-w", "t", null);
        assertEquals(RejectReason.QUEUE_FULL, full.getRejectReason());
        assertEquals(before.rejectedCapacityTotal() + 1, q2.stats().rejectedCapacityTotal());

        Thread.sleep(30); // 等 task2 退避到期
        processOne(q2, h2); // task2 重试成功
        processOne(q2, h2); // task3
        processOne(q2, h2); // task4
        assertEquals(4, q2.committedOffset());
        assertEquals(4, q2.stats().succeeded());
        assertEquals(4, h1.sideEffects.get() + h2.sideEffects.get(),
                "each accepted task's side effect executed exactly once across restart");
        long id5 = q2.enqueue("mc-5", "t", null).getTaskId();
        assertEquals(5, id5, "id sequence continues");
        q2.enqueue("mc-5", "t", null); // DUPLICATE，rejDup 再 +1
        QueueStats preFinal = q2.stats();
        log.info("[TEST-RESULT] post-restart activity: accepted={}, rejectedDup={}, rejectedCap={}, committed={}",
                preFinal.acceptedTotal(), preFinal.rejectedDuplicateTotal(),
                preFinal.rejectedCapacityTotal(), preFinal.committedOffset());

        // ---- 阶段E：收拢重新跑完（不再空转、日志真正清掉），再重启口径仍一致 ----
        var compactReport = q2.compact();
        assertEquals(0, Files.size(dir.resolve("journal.log")),
                "completed compaction must still bound local storage");
        log.info("[TEST-RESULT] compaction completed after recovery: tasksSnapshotted={}, journal {} -> 0",
                compactReport.tasksSnapshotted(), compactReport.journalBytesBefore());

        IdempotentTaskQueue q3 = TestQueueFactory.persistentQueue(props, dir);
        QueueStats fin = q3.stats();
        log.info("[TEST-OFFSET] second restart: accepted={}, rejectedDup={}, rejectedCap={}, committed={}",
                fin.acceptedTotal(), fin.rejectedDuplicateTotal(), fin.rejectedCapacityTotal(),
                fin.committedOffset());
        assertEquals(preFinal.acceptedTotal(), fin.acceptedTotal());
        assertEquals(preFinal.rejectedDuplicateTotal(), fin.rejectedDuplicateTotal());
        assertEquals(preFinal.rejectedCapacityTotal(), fin.rejectedCapacityTotal());
        assertEquals(preFinal.committedOffset(), fin.committedOffset());
        assertEquals(preFinal.succeeded(), fin.succeeded());
        // 用过的业务键仍然拦截重复投递
        var dupAfter = q3.enqueue("mc-1", "t", null);
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupAfter.getRejectReason());
        assertEquals(1, dupAfter.getExistingTaskId());
        // 队列继续消费
        var h3 = new TestHandlers.CountingHandler();
        processOne(q3, h3);
        assertEquals(5, q3.committedOffset());
        log.info("[TEST-BASIS] REJECT 帧携带单调 rejectSeq、快照记录 lastRejectSeq 高水位 => "
                + "收拢在 rename 与清日志之间被杀，重放时 rejectSeq<=高水位 的重叠帧跳过，"
                + "拒绝计数不重不漏；任务事件仍按记录版本号幂等");
    }

    @Test
    void legacyRejectFramesWithV1Snapshot_refuseStartupWithExplanation() throws Exception {
        QueueProperties props = TestQueueFactory.props(10, 1000, 3, 20, 60);
        var holder1 = TestQueueFactory.openPersistent(props, dir);
        IdempotentTaskQueue q1 = holder1.queue();
        q1.enqueue("leg-1", "t", null);
        q1.enqueue("leg-1", "t", null); // DUPLICATE #1
        q1.compact();                   // v2 快照（含拒绝计数），日志已清
        holder1.persistence().close();

        // 手工把 v2 快照降级为 v1（去掉 lastRejectSeq 字段，重算长度与 CRC）
        Path snapshot = dir.resolve("snapshot.dat");
        byte[] v2 = Files.readAllBytes(snapshot);
        java.nio.ByteBuffer head = java.nio.ByteBuffer.wrap(v2);
        int magic = head.getInt();
        head.getInt(); // 原版本号
        int v2Len = head.getInt();
        head.getLong(); // 原 CRC
        byte[] v2Payload = new byte[v2Len];
        head.get(v2Payload);
        // v1 payload = 前 5 个 long（去掉第 6 个 lastRejectSeq）+ 其余记录部分
        byte[] v1Payload = new byte[v2Len - Long.BYTES];
        System.arraycopy(v2Payload, 0, v1Payload, 0, Long.BYTES * 5);
        System.arraycopy(v2Payload, Long.BYTES * 6, v1Payload, Long.BYTES * 5,
                v2Len - Long.BYTES * 6);
        CRC32 crc = new CRC32();
        crc.update(v1Payload);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeInt(magic);
        out.writeInt(1); // v1
        out.writeInt(v1Payload.length);
        out.writeLong(crc.getValue());
        out.write(v1Payload);
        out.flush();
        Files.write(snapshot, bos.toByteArray(), StandardOpenOption.TRUNCATE_EXISTING);

        // 手工写一条旧格式 REJECT 帧（只有 kind，无 rejectSeq）
        byte[] legacyPayload = {6, 0}; // TYPE_REJECT, REJECT_DUPLICATE
        CRC32 frameCrc = new CRC32();
        frameCrc.update(legacyPayload);
        ByteArrayOutputStream fb = new ByteArrayOutputStream();
        DataOutputStream fout = new DataOutputStream(fb);
        fout.writeInt(legacyPayload.length);
        fout.writeLong(frameCrc.getValue());
        fout.write(legacyPayload);
        fout.flush();
        Files.write(dir.resolve("journal.log"), fb.toByteArray(), StandardOpenOption.TRUNCATE_EXISTING);
        log.info("[TEST-INPUT] downgraded snapshot to v1 (no rejectSeq high-water) + "
                + "legacy REJECT frame without sequence: rejection may or may not be counted in snapshot");

        RecoveryException ex = assertThrows(RecoveryException.class,
                () -> TestQueueFactory.persistentQueue(props, dir),
                "ambiguous legacy state must refuse startup");
        log.info("[TEST-RESULT] startup refused: {}", ex.getMessage());
        assertTrue(ex.getMessage().contains("ambiguous"), "explains the ambiguity");
        assertTrue(ex.getMessage().contains("refusing to start"), "explains the decision");
        log.info("[TEST-BASIS] v1 快照无 rejectSeq 高水位 + 旧格式 REJECT 帧无序号 => "
                + "计入会重、不计会漏，无法确定口径，拒绝启动而非悄悄继续");
    }

    /** 一致性判据：committed 之前全终态（位点绝不超前于任务状态）。 */
    private static void assertConsistent(IdempotentTaskQueue q) {
        long committed = q.committedOffset();
        for (var v : q.listTasks()) {
            if (v.id() <= committed) {
                assertTrue(v.status().isTerminal(),
                        "task " + v.id() + " below committed offset must be terminal");
            }
        }
    }

    private static void processOne(IdempotentTaskQueue q, TestHandlers.CountingHandler h) {
        TaskRecord claimed = q.claimNext(Instant.now()).orElseThrow();
        TestQueueFactory.worker(q, h).processOnce(claimed);
    }
}
