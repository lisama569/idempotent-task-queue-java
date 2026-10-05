package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.QueueStats;
import com.github.lisama569.idempotenttaskqueuejava.domain.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.persistence.QueuePersistence;
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
 * A) 收拢在“快照已原子替换、日志尚未清空”之间被杀 → 重启后重复投递/容量拒绝计数、
 *    各状态数量、消费位点、业务键查重结果与停机前完全一致（同一次拒绝不得重复计数），
 *    且恢复后投递/消费/重试/再次收拢全部照常；
 * B) 收拢在更早阶段被杀（snapshot.tmp 残留、正式快照未替换）→ tmp 被丢弃，状态与计数不变；
 * C) 旧格式（无 rejectSeq）拒绝帧与快照同时出现 → 覆盖关系无法判定，明确拒绝启动并说明依据；
 * D) 旧格式拒绝帧在无快照时出现 → 不存在重复计数来源，安全应用并可继续运行。
 */
class CrashMidCompactionRecoveryTest {
    private static final Logger log = LoggerFactory.getLogger(CrashMidCompactionRecoveryTest.class);

    @TempDir
    Path dir;

    @Test
    void crashAfterSnapshotRename_rejectCountsAndStateIdentical_andQueueKeepsWorking() throws Exception {
        QueueProperties props = TestQueueFactory.props(4, 1000, 3, 20, 60);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        TestHandlers.CountingHandler h1 = new TestHandlers.CountingHandler();

        // 制造历史：4 个接受 + 2 个容量拒绝 + 2 个重复投递拒绝；1 成功终态 + 1 退避中
        long id1 = q1.enqueue("mc-1", "t", null).getTaskId();
        long id2 = q1.enqueue("mc-2", "t", null).getTaskId();
        q1.enqueue("mc-3", "t", null);
        q1.enqueue("mc-4", "t", null);
        assertEquals(RejectReason.QUEUE_FULL, q1.enqueue("mc-5", "t", null).getRejectReason());
        assertEquals(RejectReason.QUEUE_FULL, q1.enqueue("mc-6", "t", null).getRejectReason());
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, q1.enqueue("mc-1", "t", null).getRejectReason());
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, q1.enqueue("mc-2", "t", null).getRejectReason());
        processOne(q1, h1);                 // mc-1 成功，位点推进到 1
        h1.failTimes(id2, 1);
        processOne(q1, h1);                 // mc-2 失败一次，进入 RETRY_WAITING

        QueueStats before = q1.stats();
        var key1Before = q1.findByBusinessKey("mc-1").orElseThrow();
        var key2Before = q1.findByBusinessKey("mc-2").orElseThrow();
        assertEquals(4, before.acceptedTotal());
        assertEquals(2, before.rejectedDuplicateTotal());
        assertEquals(2, before.rejectedCapacityTotal());
        assertEquals(1, before.committedOffset());
        assertEquals(1, before.succeeded());
        assertEquals(1, before.retryWaiting());
        assertEquals(2, before.pending());
        log.info("[TEST-INPUT] 停机前: accepted={}, rejectedDup={}, rejectedCap={}, committed={}, "
                        + "succeeded={}, retryWaiting={}, pending={}, active={}",
                before.acceptedTotal(), before.rejectedDuplicateTotal(), before.rejectedCapacityTotal(),
                before.committedOffset(), before.succeeded(), before.retryWaiting(),
                before.pending(), before.activeCount());

        // 模拟“收拢在快照 rename 之后、清日志之前被强杀”：
        // 目录状态 = 新快照已生效 + 旧日志一字节未少（快照与日志完全重叠）
        byte[] journalAtCrash = Files.readAllBytes(dir.resolve("journal.log"));
        q1.compact();
        assertEquals(0, Files.size(dir.resolve("journal.log")), "compaction itself completes");
        Files.write(dir.resolve("journal.log"), journalAtCrash, StandardOpenOption.TRUNCATE_EXISTING);
        log.info("[TEST-INPUT] 模拟收拢中途被杀: snapshot.dat 已替换, journal.log 恢复为收拢前的 {} 字节"
                + "（等价于 rename 后、truncate 前崩溃）", journalAtCrash.length);

        // 重启：同一次拒绝不得重复计数，各口径与停机前一致
        IdempotentTaskQueue q2 = TestQueueFactory.persistentQueue(props, dir);
        var recovery = q2.lastRecovery();
        QueueStats after = q2.stats();
        log.info("[TEST-OFFSET] 重启恢复: snapshotLoaded={}, framesReplayed={}, eventsSkipped(已被快照覆盖)={}, "
                        + "committed={}, rejectedDup={}, rejectedCap={}",
                recovery.snapshotLoaded(), recovery.journalRecordsReplayed(),
                recovery.journalEventsSkipped(), after.committedOffset(),
                after.rejectedDuplicateTotal(), after.rejectedCapacityTotal());
        assertTrue(recovery.snapshotLoaded());
        assertEquals(8, recovery.journalEventsSkipped(),
                "4 条 ENQUEUE + 4 条 REJECT 已被快照覆盖，必须幂等跳过");
        assertEquals(before.rejectedDuplicateTotal(), after.rejectedDuplicateTotal(),
                "重复投递拒绝计数不得因收拢中断而翻倍");
        assertEquals(before.rejectedCapacityTotal(), after.rejectedCapacityTotal(),
                "容量拒绝计数不得因收拢中断而翻倍");
        assertEquals(before.acceptedTotal(), after.acceptedTotal());
        assertEquals(before.committedOffset(), after.committedOffset(), "消费位置口径一致");
        assertEquals(before.succeeded(), after.succeeded());
        assertEquals(before.retryWaiting(), after.retryWaiting());
        assertEquals(before.pending(), after.pending());
        assertEquals(before.activeCount(), after.activeCount());
        // 按业务键查重结果一致
        var key1After = q2.findByBusinessKey("mc-1").orElseThrow();
        var key2After = q2.findByBusinessKey("mc-2").orElseThrow();
        assertEquals(key1Before.id(), key1After.id());
        assertEquals(key1Before.status(), key1After.status());
        assertEquals(key2Before.id(), key2After.id());
        assertEquals(key2Before.status(), key2After.status());
        log.info("[TEST-RESULT] 重启前后口径一致: rejectedDup={}=={}, rejectedCap={}=={}, committed={}=={}, "
                        + "mc-1->id{}/{}, mc-2->id{}/{}",
                before.rejectedDuplicateTotal(), after.rejectedDuplicateTotal(),
                before.rejectedCapacityTotal(), after.rejectedCapacityTotal(),
                before.committedOffset(), after.committedOffset(),
                key1After.id(), key1After.status(), key2After.id(), key2After.status());

        // 恢复后照常工作：新重复拒绝/容量拒绝各自只 +1，退避重试与新任务消费正常
        TestHandlers.CountingHandler h2 = new TestHandlers.CountingHandler();
        var dup = q2.enqueue("mc-1", "t", null);
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dup.getRejectReason());
        assertEquals(id1, dup.getExistingTaskId());
        assertEquals(3, q2.stats().rejectedDuplicateTotal(), "新的重复拒绝只计一次");
        long id7 = q2.enqueue("mc-7", "t", null).getTaskId(); // active 3 -> 4
        assertEquals(RejectReason.QUEUE_FULL, q2.enqueue("mc-8", "t", null).getRejectReason());
        assertEquals(3, q2.stats().rejectedCapacityTotal(), "新的容量拒绝只计一次");

        Thread.sleep(30); // 等 mc-2 退避到期
        processOne(q2, h2); // mc-2 重试成功
        h2.failTimes(3, 1);
        processOne(q2, h2); // mc-3 失败一次（失败重试照常）
        assertEquals(TaskStatus.RETRY_WAITING, q2.getTask(3).status());
        Thread.sleep(30);
        processOne(q2, h2); // mc-3 重试成功
        processOne(q2, h2); // mc-4 成功
        processOne(q2, h2); // mc-7 成功
        assertEquals(5, q2.committedOffset());
        assertEquals(5, q2.stats().succeeded());
        assertEquals(4, h2.sideEffects.get(), "恢复后每个任务副作用恰好一次");
        log.info("[TEST-RESULT] 恢复后继续投递/消费/重试正常: committed=5, succeeded=5, "
                + "rejectedDup=3, rejectedCap=3");

        // 收拢仍然起控制体积的作用：再次收拢清空日志，重启后口径仍一致
        QueuePersistence.CompactionReport second = q2.compact();
        assertEquals(5, second.tasksSnapshotted());
        assertEquals(0, Files.size(dir.resolve("journal.log")), "收拢后日志被清空，历史不无限增长");
        IdempotentTaskQueue q3 = TestQueueFactory.persistentQueue(props, dir);
        QueueStats finalStats = q3.stats();
        assertEquals(5, finalStats.acceptedTotal());
        assertEquals(3, finalStats.rejectedDuplicateTotal());
        assertEquals(3, finalStats.rejectedCapacityTotal());
        assertEquals(5, finalStats.committedOffset());
        assertEquals(5, finalStats.succeeded());
        var dupAgain = q3.enqueue("mc-2", "t", null);
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dupAgain.getRejectReason());
        assertEquals(id2, dupAgain.getExistingTaskId(), "用过的业务键仍然拦截重复投递");
        assertEquals(6, q3.enqueue("mc-9", "t", null).getTaskId(), "ID 序列不回退");
        log.info("[TEST-BASIS] REJECT 帧携带全局单调 rejectSeq，快照拒绝计数之和=已覆盖序号上限，"
                + "重放只应用超出上限的事件 => 收拢中途被杀不重复计数；再次收拢+重启后口径仍一致");
    }

    @Test
    void crashBeforeSnapshotRename_tmpDiscarded_stateAndCountsIntact() throws Exception {
        QueueProperties props = TestQueueFactory.props(4, 1000, 3, 20, 60);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        TestHandlers.CountingHandler h1 = new TestHandlers.CountingHandler();
        q1.enqueue("tmp-1", "t", null);
        q1.enqueue("tmp-2", "t", null);
        q1.enqueue("tmp-1", "t", null); // 重复拒绝 +1
        processOne(q1, h1);
        QueueStats before = q1.stats();
        assertEquals(1, before.rejectedDuplicateTotal());
        log.info("[TEST-INPUT] 停机前: accepted={}, rejectedDup={}, committed={}",
                before.acceptedTotal(), before.rejectedDuplicateTotal(), before.committedOffset());

        // 模拟“收拢在写 snapshot.tmp 阶段被杀”：tmp 残留、正式快照未替换、日志完整
        Files.write(dir.resolve("snapshot.tmp"), new byte[]{0x1, 0x2, 0x3},
                StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        log.info("[TEST-INPUT] 模拟收拢早期被杀: 残留 snapshot.tmp（未 rename），journal.log 保持完整");

        IdempotentTaskQueue q2 = TestQueueFactory.persistentQueue(props, dir);
        var recovery = q2.lastRecovery();
        QueueStats after = q2.stats();
        log.info("[TEST-OFFSET] 重启恢复: snapshotLoaded={}, framesReplayed={}, committed={}, rejectedDup={}",
                recovery.snapshotLoaded(), recovery.journalRecordsReplayed(),
                after.committedOffset(), after.rejectedDuplicateTotal());
        assertFalse(recovery.snapshotLoaded(), "tmp 未 rename，正式快照不存在");
        assertFalse(Files.exists(dir.resolve("snapshot.tmp")), "残留 tmp 必须被丢弃");
        assertEquals(before.acceptedTotal(), after.acceptedTotal());
        assertEquals(before.rejectedDuplicateTotal(), after.rejectedDuplicateTotal());
        assertEquals(before.rejectedCapacityTotal(), after.rejectedCapacityTotal());
        assertEquals(before.committedOffset(), after.committedOffset());
        assertEquals(before.succeeded(), after.succeeded());
        assertEquals(before.pending(), after.pending());

        // 恢复后收拢可重新跑完，且再次重启口径不变
        processOne(q2, h1);
        q2.compact();
        IdempotentTaskQueue q3 = TestQueueFactory.persistentQueue(props, dir);
        assertEquals(2, q3.stats().acceptedTotal());
        assertEquals(1, q3.stats().rejectedDuplicateTotal());
        assertEquals(2, q3.committedOffset());
        log.info("[TEST-BASIS] tmp 未原子替换即不算数（直接丢弃），正式快照与日志二者皆完整 => 状态确定，"
                + "清理可从头重跑");
    }

    @Test
    void legacyRejectFrameWithSnapshot_refusesStartup_withExplanation() throws Exception {
        QueueProperties props = TestQueueFactory.props(10, 1000, 3, 20, 60);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        q1.enqueue("lg-1", "t", null);
        q1.enqueue("lg-1", "t", null); // 新格式 REJECT 帧（带 rejectSeq）
        q1.compact();
        assertTrue(Files.exists(dir.resolve("snapshot.dat")));

        // 在日志尾部拼接一条旧格式 REJECT 帧（只有 kind，无 rejectSeq）：
        // 快照已存在时无法判定它是否已计入快照计数 —— 矛盾状态
        appendLegacyRejectFrame(dir.resolve("journal.log"), 1);
        log.info("[TEST-INPUT] 快照存在的前提下向日志追加一条无 rejectSeq 的旧格式 REJECT 帧");

        RecoveryException ex = assertThrows(RecoveryException.class,
                () -> TestQueueFactory.persistentQueue(props, dir),
                "覆盖关系不可判定的矛盾状态必须拒绝启动");
        log.info("[TEST-RESULT] 拒绝启动: {}", ex.getMessage());
        assertTrue(ex.getMessage().contains("legacy REJECT"), "说明卡在旧格式拒绝帧");
        assertTrue(ex.getMessage().contains("refusing to start"), "说明判断结论");
        log.info("[TEST-BASIS] 旧帧无序号、快照计数无法与之比对 => 计数必然有歧义，"
                + "宁可拒绝启动也不用可能重复计数的状态对外服务");
    }

    @Test
    void legacyRejectFrameWithoutSnapshot_appliesSafely_andMixedJournalReplays() throws Exception {
        QueueProperties props = TestQueueFactory.props(10, 1000, 3, 20, 60);
        IdempotentTaskQueue q1 = TestQueueFactory.persistentQueue(props, dir);
        q1.enqueue("nm-1", "t", null);
        // 无快照：旧格式帧不存在“已计入快照”的来源，可安全应用
        appendLegacyRejectFrame(dir.resolve("journal.log"), 0);
        log.info("[TEST-INPUT] 无快照目录中混入一条旧格式重复拒绝帧（升级前的历史数据）");

        IdempotentTaskQueue q2 = TestQueueFactory.persistentQueue(props, dir);
        assertEquals(1, q2.stats().rejectedDuplicateTotal(), "旧格式拒绝帧被应用一次");
        assertEquals(1, q2.stats().acceptedTotal());
        // 新旧格式混排：新拒绝从 rejectSeq=2 继续编号，再次重启重放不重复、不断裂
        q2.enqueue("nm-1", "t", null);
        q2.enqueue("nm-9", "t", null);
        q2.enqueue("nm-9", "t", null);
        assertEquals(3, q2.stats().rejectedDuplicateTotal());

        IdempotentTaskQueue q3 = TestQueueFactory.persistentQueue(props, dir);
        assertEquals(3, q3.stats().rejectedDuplicateTotal(), "混排日志重放后计数一致");
        assertEquals(2, q3.stats().acceptedTotal());
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY,
                q3.enqueue("nm-9", "t", null).getRejectReason());
        log.info("[TEST-BASIS] 无快照 => 旧帧不可能被重复计数，安全应用；后续新帧连续编号，"
                + "混排日志可反复重放");
    }

    /** 手工拼接一条旧格式（无 rejectSeq）的 REJECT 帧：payload = [TYPE_REJECT][kind]。 */
    private static void appendLegacyRejectFrame(Path journal, int rejectKind) throws Exception {
        byte[] payload = new byte[]{6 /* TYPE_REJECT */, (byte) rejectKind};
        CRC32 crc = new CRC32();
        crc.update(payload);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeInt(payload.length);
        out.writeLong(crc.getValue());
        out.write(payload);
        out.flush();
        Files.write(journal, bos.toByteArray(), StandardOpenOption.APPEND);
    }

    private static void processOne(IdempotentTaskQueue q, TestHandlers.CountingHandler h) {
        TaskRecord claimed = q.claimNext(Instant.now()).orElseThrow();
        TestQueueFactory.worker(q, h).processOnce(claimed);
    }
}
