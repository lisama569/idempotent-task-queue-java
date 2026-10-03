package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.persistence.QueuePersistence;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 历史收拢（compaction）场景：
 * 收拢前后语义一致（状态/位点/去重/计数），收拢后新的重复投递与失败重试不受影响，
 * 收拢后重启从快照+增量日志恢复；日志超阈值自动收拢。
 */
class CompactionTest {
    private static final Logger log = LoggerFactory.getLogger(CompactionTest.class);

    @TempDir
    Path dir;

    @Test
    void compaction_preservesSemantics_andSurvivesRestart() throws Exception {
        QueueProperties props = TestQueueFactory.props(10, 1000, 3, 20, 60);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        TestHandlers.CountingHandler h1 = new TestHandlers.CountingHandler();

        long id1 = q1.enqueue("cp-1", "t", null).getTaskId();
        long id2 = q1.enqueue("cp-2", "t", null).getTaskId();
        long id3 = q1.enqueue("cp-3", "t", null).getTaskId();
        q1.enqueue("cp-1", "t", null); // 重复投递，计入拒绝计数
        // id1 成功；id2 失败一次进入退避（收拢时处于 RETRY_WAITING）
        processOne(q1, h1);
        h1.failTimes(id2, 1);
        processOne(q1, h1);
        assertEquals(TaskStatus.RETRY_WAITING, q1.getTask(id2).status());
        long journalBefore = Files.size(dir.resolve("journal.log"));
        log.info("[TEST-INPUT] 3 tasks (SUCCEEDED/RETRY_WAITING/PENDING) + 1 duplicate reject, journalBytes={}",
                journalBefore);

        // 手动收拢
        QueuePersistence.CompactionReport compactReport = q1.compact();
        log.info("[TEST-RESULT] compacted: tasksSnapshotted={}, journal {} -> {} bytes, committed={}",
                compactReport.tasksSnapshotted(), compactReport.journalBytesBefore(),
                compactReport.journalBytesAfter(), compactReport.committedOffset());
        assertEquals(3, compactReport.tasksSnapshotted());
        assertEquals(0, Files.size(dir.resolve("journal.log")), "journal cleared after compaction");
        assertTrue(Files.exists(dir.resolve("snapshot.dat")));

        // 收拢后正常投递与消费不受影响：重复投递仍被拒、退避任务可重试成功
        var dup = q1.enqueue("cp-1", "t", null);
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dup.getRejectReason());
        assertEquals(id1, dup.getExistingTaskId());
        Thread.sleep(30); // 等 id2 退避到期
        processOne(q1, h1); // id2 重试成功
        processOne(q1, h1); // id3 成功
        assertEquals(3, q1.committedOffset());
        var statsBefore = q1.stats();
        log.info("[TEST-OFFSET] post-compaction activity done: committed={}, accepted={}, rejectedDup={}",
                statsBefore.committedOffset(), statsBefore.acceptedTotal(),
                statsBefore.rejectedDuplicateTotal());

        // 重启：快照 + 收拢后的增量日志一起恢复，口径与收拢前一致
        IdempotentTaskQueue q2 = TestQueueFactory.persistentQueue(props, dir);
        var recovery = q2.lastRecovery();
        var statsAfter = q2.stats();
        log.info("[TEST-OFFSET] restart after compaction: snapshotLoaded={}, snapshotTasks={}, "
                        + "journalFramesReplayed={}, committed={}, accepted={}, rejectedDup={}",
                recovery.snapshotLoaded(), recovery.snapshotTaskCount(),
                recovery.journalRecordsReplayed(), statsAfter.committedOffset(),
                statsAfter.acceptedTotal(), statsAfter.rejectedDuplicateTotal());
        assertTrue(recovery.snapshotLoaded(), "recovery must use snapshot");
        assertTrue(recovery.journalRecordsReplayed() > 0, "post-compaction journal delta replayed");
        assertEquals(statsBefore.committedOffset(), statsAfter.committedOffset());
        assertEquals(statsBefore.acceptedTotal(), statsAfter.acceptedTotal());
        assertEquals(statsBefore.rejectedDuplicateTotal(), statsAfter.rejectedDuplicateTotal());
        assertEquals(3, statsAfter.succeeded());
        // 收拢+重启后去重口径不变
        var dupAfter = q2.enqueue("cp-2", "t", null);
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupAfter.getRejectReason());
        assertEquals(id2, dupAfter.getExistingTaskId());
        // 新任务可继续投递，ID 序列不回退
        long id4 = q2.enqueue("cp-4", "t", null).getTaskId();
        assertEquals(4, id4, "id sequence continues after snapshot restore");
        log.info("[TEST-BASIS] 快照原子替换 + 版本号幂等重放 => 收拢前后语义一致，收拢中途崩溃也不重复应用");
    }

    @Test
    void autoCompaction_triggersAtThreshold_withoutDisturbingTraffic() throws Exception {
        QueueProperties props = TestQueueFactory.props(100, 1000, 3, 20, 60);
        props.setJournalCompactThresholdBytes(256); // 极小阈值，几次写入即触发
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        TestHandlers.CountingHandler h1 = new TestHandlers.CountingHandler();

        for (int i = 1; i <= 8; i++) {
            q1.enqueue("auto-" + i, "t", "payload-" + i);
            processOne(q1, h1);
        }
        assertTrue(Files.exists(dir.resolve("snapshot.dat")),
                "auto compaction must have produced a snapshot");
        assertEquals(8, q1.committedOffset(), "traffic undisturbed by auto compaction");
        assertEquals(8, q1.stats().succeeded());
        log.info("[TEST-RESULT] auto compaction at threshold=256B: 8 tasks processed, committed=8, "
                + "journalBytes={}", Files.size(dir.resolve("journal.log")));

        IdempotentTaskQueue q2 = TestQueueFactory.persistentQueue(props, dir);
        assertEquals(8, q2.committedOffset());
        assertEquals(8, q2.stats().succeeded());
        assertEquals(8, q2.stats().acceptedTotal());
        log.info("[TEST-BASIS] 收拢在存储锁内取快照 => 与并发投递/消费不冲突，恢复后计数连续");
    }

    private static void processOne(IdempotentTaskQueue q, TestHandlers.CountingHandler h) {
        TaskRecord claimed = q.claimNext(Instant.now()).orElseThrow();
        TestQueueFactory.worker(q, h).processOnce(claimed);
    }
}
