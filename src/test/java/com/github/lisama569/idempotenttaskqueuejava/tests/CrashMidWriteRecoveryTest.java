package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.persistence.RecoveryException;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * “正在保存的时候被杀掉”场景：
 * A) 日志帧尾撕裂（写一半死掉）→ 截断到最后一条完整帧，恢复为一致前缀，可继续处理；
 * B) 完整帧但内容损坏（非撕裂能产生）→ 明确拒绝启动并说明位置与依据；
 * C) 快照损坏 → 明确拒绝启动。
 */
class CrashMidWriteRecoveryTest {
    private static final Logger log = LoggerFactory.getLogger(CrashMidWriteRecoveryTest.class);

    @TempDir
    Path dir;

    @Test
    void tornJournalTail_recoversToConsistentPrefix_andContinues() throws Exception {
        QueueProperties props = TestQueueFactory.props(10, 1000, 3, 20, 60);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        TestHandlers.CountingHandler h1 = new TestHandlers.CountingHandler();
        q1.enqueue("torn-1", "t", null);
        ConsumerWorkerHolder.processOne(q1, h1);

        q1.enqueue("torn-2", "t", null);
        q1.enqueue("torn-3", "t", null);
        long committedBefore = q1.committedOffset();
        long journalSizeBefore = Files.size(dir.resolve("journal.log"));
        log.info("[TEST-INPUT] 3 tasks, task1 SUCCEEDED, committed={}, journalBytes={}",
                committedBefore, journalSizeBefore);

        // 模拟“写日志写到一半被杀”：在帧尾追加半条帧（半个帧头 + 垃圾字节）
        byte[] halfFrame = new byte[]{0, 0, 0, 40, 0, 1, 2}; // 声称长度40，实际只有7字节
        Files.write(dir.resolve("journal.log"), halfFrame, StandardOpenOption.APPEND);
        log.info("[TEST-INPUT] appended {} torn bytes to journal tail (simulating kill mid-write)",
                halfFrame.length);

        // 恢复：撕裂尾部被截断，状态是“一致前缀”
        IdempotentTaskQueue q2 = TestQueueFactory.persistentQueue(props, dir);
        var report = q2.lastRecovery();
        log.info("[TEST-OFFSET] recovery: truncatedAt={}, tornBytes={}, committed={}, tasksRecovered={}, basis={}",
                report.journalTruncatedAtBytes(), report.journalTornTailBytes(),
                report.committedOffset(), report.tasksRecovered(), report.basis());
        assertEquals(journalSizeBefore, report.journalTruncatedAtBytes(), "truncate at last complete frame");
        assertEquals(halfFrame.length, report.journalTornTailBytes());
        assertEquals(committedBefore, q2.committedOffset(), "offset not ahead of task states");
        assertEquals(3, q2.stats().acceptedTotal(), "no task lost");
        assertEquals(TaskStatus.SUCCEEDED, q2.getTask(1).status());
        assertEquals(TaskStatus.PENDING, q2.getTask(2).status());
        // 一致性判据：位点之前全部终态，位点之后全部非终态
        assertConsistent(q2);

        // 截断后日志可继续追加，队列可继续正常处理
        TestHandlers.CountingHandler h2 = new TestHandlers.CountingHandler();
        ConsumerWorkerHolder.processOne(q2, h2);
        ConsumerWorkerHolder.processOne(q2, h2);
        assertEquals(3, q2.committedOffset());
        assertEquals(3, q2.stats().succeeded());
        assertEquals(2, h2.sideEffects.get());

        // 再次重启验证截断后的日志本身也是良构的
        IdempotentTaskQueue q3 = TestQueueFactory.persistentQueue(props, dir);
        assertEquals(3, q3.committedOffset());
        assertEquals(3, q3.stats().succeeded());
        assertConsistent(q3);
        log.info("[TEST-BASIS] 帧带长度+CRC，帧尾不完整即撕裂写 => 截断到最后完整帧；"
                + "终态与位点同帧 => 前缀恢复不会出现位点超前");
    }

    @Test
    void corruptCompleteFrame_refusesStartup_withExplanation() throws Exception {
        QueueProperties props = TestQueueFactory.props(10, 1000, 3, 20, 60);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        q1.enqueue("bad-1", "t", null);
        q1.enqueue("bad-2", "t", null);

        // 篡改第一条完整帧的内容（保持长度不变，CRC 必然不符）——这不是撕裂写能产生的状态
        Path journal = dir.resolve("journal.log");
        byte[] bytes = Files.readAllBytes(journal);
        int payloadStart = Integer.BYTES + Long.BYTES + 2; // 跳过帧头与事件类型，改任务ID区域
        bytes[payloadStart] ^= 0x7F;
        Files.write(journal, bytes, StandardOpenOption.TRUNCATE_EXISTING);
        log.info("[TEST-INPUT] flipped one byte inside a complete journal frame at offset {}", payloadStart);

        RecoveryException ex = assertThrows(RecoveryException.class,
                () -> TestQueueFactory.persistentQueue(props, dir),
                "complete-but-corrupt frame must refuse startup");
        log.info("[TEST-RESULT] startup refused: {}", ex.getMessage());
        assertTrue(ex.getMessage().contains("corrupt"), "explains corruption location");
        assertTrue(ex.getMessage().contains("refusing to start"), "explains decision");
        log.info("[TEST-BASIS] 完整帧 CRC 不符不可能来自追加写中断 => 状态有歧义，拒绝启动而非悄悄糊过去");
    }

    @Test
    void corruptSnapshot_refusesStartup_withExplanation() throws Exception {
        QueueProperties props = TestQueueFactory.props(10, 1000, 3, 20, 60);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        q1.enqueue("snap-1", "t", null);
        q1.compact();
        assertTrue(Files.exists(dir.resolve("snapshot.dat")));

        byte[] bytes = Files.readAllBytes(dir.resolve("snapshot.dat"));
        bytes[bytes.length - 1] ^= 0x5A; // 篡改快照尾部
        Files.write(dir.resolve("snapshot.dat"), bytes, StandardOpenOption.TRUNCATE_EXISTING);
        log.info("[TEST-INPUT] flipped last byte of snapshot.dat after compaction");

        RecoveryException ex = assertThrows(RecoveryException.class,
                () -> TestQueueFactory.persistentQueue(props, dir));
        log.info("[TEST-RESULT] startup refused: {}", ex.getMessage());
        assertTrue(ex.getMessage().contains("snapshot"), "points at snapshot as the problem");
        log.info("[TEST-BASIS] 收拢后快照是唯一状态来源，CRC 不符 => 拒绝启动并说明卡在哪");
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

    /** 小工具：同步处理一个任务（领取+执行+ACK）。 */
    private static final class ConsumerWorkerHolder {
        static void processOne(IdempotentTaskQueue q, TestHandlers.CountingHandler h) {
            TaskRecord claimed = q.claimNext(Instant.now()).orElseThrow();
            TestQueueFactory.worker(q, h).processOnce(claimed);
        }
    }
}
