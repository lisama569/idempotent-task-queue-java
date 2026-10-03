package com.github.lisama569.idempotenttaskqueuejava.persistence;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.store.StoreListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 持久化编排：把 TaskStore/OffsetManager 的变更事件写入本地日志（每次 fsync），
 * 重启时从“快照 + 日志”恢复，并按阈值触发历史收拢（快照压缩 + 日志重置）。
 *
 * <p>崩溃安全不变量：</p>
 * <ul>
 *   <li>同一任务先写 STATUS(终态) 再写 OFFSET，因此崩溃后只可能“位点滞后于终态”，
 *       恢复时把位点确定性地推进到最长终态前缀即可；</li>
 *   <li>若发现“位点越过未终态任务”或“位点超过最大任务ID”，说明数据自相矛盾，
 *       抛 {@link RecoveryException} 拒绝启动并说明卡点；</li>
 *   <li>恢复时仍处于 DELIVERING 的任务说明上次进程在投递途中消失，
 *       确定性地重置为 PENDING（staleDeliveries+1）等待重新领取；</li>
 *   <li>所有事件应用幂等，压缩（写快照→重置日志）中途崩溃后重放旧事件不产生矛盾。</li>
 * </ul>
 */
public class PersistenceManager implements StoreListener, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(PersistenceManager.class);

    public static final String JOURNAL_FILE = "journal.log";

    private final Path dir;
    private final long compactThresholdBytes;
    private final SnapshotStore snapshots;
    private final Journal journal;

    /** 镜像状态：与日志事件流严格同步，压缩时直接物化为快照。 */
    private final Map<Long, TaskSnapshot> tasks = new LinkedHashMap<>();
    private long sequence;
    private long committedOffset;
    private long acceptedTotal;
    private long rejectedDuplicateTotal;
    private long rejectedCapacityTotal;

    private RecoveryReport lastRecovery;
    private RecoveredState recoveredState = RecoveredState.empty();
    /** 测试钩子：置位后下一次位点提交不落盘，模拟“状态已写、位点未写”的崩溃点。 */
    private volatile boolean skipNextOffsetWrite;

    public PersistenceManager(Path dir, long compactThresholdBytes) {
        this.dir = dir;
        this.compactThresholdBytes = compactThresholdBytes;
        this.snapshots = new SnapshotStore(dir);
        this.journal = new Journal(dir.resolve(JOURNAL_FILE));
    }

    /**
     * 从本地目录恢复状态；目录不存在或为空视为首次启动（兼容全新部署）。
     * 恢复完成后管理器进入可追加状态，镜像与磁盘一致。
     */
    public synchronized RecoveredState recover() {
        List<String> notes = new ArrayList<>();
        try {
            Files.createDirectories(dir);
        } catch (Exception e) {
            throw new RecoveryException("cannot create persistence dir: " + dir, e);
        }

        // 1) 快照
        boolean snapshotUsed = false;
        long snapshotCreatedAt = 0L;
        int snapshotTaskCount = 0;
        var snapshotOpt = snapshots.read();
        if (snapshotOpt.isPresent()) {
            var s = snapshotOpt.get();
            snapshotUsed = true;
            snapshotCreatedAt = s.createdAtEpochMillis();
            snapshotTaskCount = s.tasks().size();
            committedOffset = s.committedOffset();
            sequence = s.sequence();
            acceptedTotal = s.acceptedTotal();
            rejectedDuplicateTotal = s.rejectedDuplicateTotal();
            rejectedCapacityTotal = s.rejectedCapacityTotal();
            for (TaskSnapshot t : s.tasks()) {
                tasks.put(t.id(), t);
            }
            notes.add("loaded snapshot: tasks=" + s.tasks().size()
                    + ", committedOffset=" + s.committedOffset() + ", sequence=" + s.sequence());
        } else {
            notes.add("no snapshot found, starting from journal only (or fresh dir)");
        }

        // 2) 日志重放（事件应用幂等，可叠在快照状态上）
        List<JournalEvent> events = journal.replay();
        for (JournalEvent e : events) {
            apply(e);
        }
        long torn = journal.lastTornTailBytes();
        if (torn > 0) {
            notes.add("truncated torn journal tail: " + torn + " bytes (crash during append)");
        }
        notes.add("replayed journal: events=" + events.size()
                + ", bytes=" + journal.lastReplayedBytes());

        // 3) 一致性校验与确定性修复
        validateNoDuplicateBusinessKeys();
        if (committedOffset > sequence) {
            throw new RecoveryException("committed offset " + committedOffset
                    + " exceeds max task id " + sequence
                    + " (offset moved past tasks that do not exist); refusing to start");
        }
        for (Map.Entry<Long, TaskSnapshot> e : tasks.entrySet()) {
            if (e.getKey() <= committedOffset && !e.getValue().status().isTerminal()) {
                throw new RecoveryException("task " + e.getKey() + " is " + e.getValue().status()
                        + " but committed offset is already " + committedOffset
                        + " (offset moved past an unfinished task); refusing to start");
            }
        }

        // 3a) 崩溃时处于 DELIVERING 的任务：投递者已随进程消失，确定性重置为 PENDING
        int deliveringReset = 0;
        Instant now = Instant.now();
        List<TaskSnapshot> resets = new ArrayList<>();
        for (TaskSnapshot t : tasks.values()) {
            if (t.status() == TaskStatus.DELIVERING) {
                TaskSnapshot reset = new TaskSnapshot(t.id(), t.businessKey(), t.type(), t.payload(),
                        t.maxAttempts(), t.enqueuedAt(), TaskStatus.PENDING, t.attempts(),
                        now, t.lastFailureAt(), t.lastErrorMessage(), t.sideEffectDone(),
                        t.staleDeliveries() + 1, t.version() + 1);
                tasks.put(t.id(), reset);
                resets.add(reset);
                deliveringReset++;
            }
        }
        for (TaskSnapshot r : resets) {
            journal.append(statusEvent(r));
        }
        if (deliveringReset > 0) {
            notes.add("reset " + deliveringReset
                    + " DELIVERING task(s) to PENDING (previous process died mid-delivery)");
        }

        // 3b) 位点滞后修复：崩溃可能发生在“终态已写、位点未写”之间，
        //     把位点确定性地推进到最长终态前缀（这些任务绝不会重跑，推进是安全的）
        long offsetBeforeFix = committedOffset;
        boolean advanced = false;
        while (true) {
            TaskSnapshot next = tasks.get(committedOffset + 1);
            if (next == null || !next.status().isTerminal()) {
                break;
            }
            committedOffset++;
            advanced = true;
        }
        if (advanced) {
            journal.append(new JournalEvent.OffsetCommitted(committedOffset));
            notes.add("advanced committed offset over terminal prefix: "
                    + offsetBeforeFix + " -> " + committedOffset
                    + " (crash between terminal-status write and offset write)");
        }

        // 4) 产出恢复产物
        List<TaskRecord> records = new ArrayList<>(tasks.size());
        for (TaskSnapshot t : tasks.values()) {
            records.add(toRecord(t));
        }
        lastRecovery = new RecoveryReport(snapshotUsed, snapshotCreatedAt, snapshotTaskCount,
                journal.lastReplayedBytes(), events.size(), torn, deliveringReset,
                offsetBeforeFix, committedOffset, advanced, records.size(), sequence,
                acceptedTotal, rejectedDuplicateTotal, rejectedCapacityTotal,
                List.copyOf(notes));
        log.info("recovery finished: {}", lastRecovery.summarize());
        for (String note : notes) {
            log.info("recovery note: {}", note);
        }
        recoveredState = new RecoveredState(records, sequence, committedOffset,
                acceptedTotal, rejectedDuplicateTotal, rejectedCapacityTotal, lastRecovery);
        return recoveredState;
    }

    // ---------- StoreListener / 位点回调（均在存储锁内触发） ----------

    @Override
    public void onEnqueued(TaskRecord record, long acceptedTotal) {
        appendAndApply(new JournalEvent.TaskEnqueued(record.getId(), record.getBusinessKey(),
                record.getType(), record.getPayload(), record.getMaxAttempts(),
                record.getEnqueuedAt()));
    }

    @Override
    public void onStatusChanged(TaskRecord r) {
        appendAndApply(new JournalEvent.TaskStatusChanged(r.getId(), r.getStatus(), r.getAttempts(),
                r.getNextVisibleAt(), r.getLastFailureAt(), r.getLastErrorMessage(),
                r.isSideEffectDone(), r.getStaleDeliveries(), r.getVersion()));
    }

    @Override
    public void onDuplicateRejected(long rejectedDuplicateTotal) {
        appendAndApply(new JournalEvent.CountersUpdated(acceptedTotal,
                rejectedDuplicateTotal, rejectedCapacityTotal));
    }

    @Override
    public void onCapacityRejected(long rejectedCapacityTotal) {
        appendAndApply(new JournalEvent.CountersUpdated(acceptedTotal,
                rejectedDuplicateTotal, rejectedCapacityTotal));
    }

    /** OffsetManager 提交位点后的回调（在存储锁内）。 */
    public void onOffsetCommitted(long committed) {
        if (skipNextOffsetWrite) {
            skipNextOffsetWrite = false;
            log.warn("SIMULATED CRASH: offset write skipped after terminal status was persisted"
                    + " (committed={})", committed);
            return;
        }
        appendAndApply(new JournalEvent.OffsetCommitted(committed));
    }

    // ---------- 历史收拢 ----------

    /**
     * 立即执行一次历史收拢：把当前镜像物化为快照（原子 rename），再重置日志。
     * 被收拢的只是“已被后续状态覆盖的历史变迁”，任务记录、业务键、位点、计数器
     * 全部保留在快照中，因此收拢前后语义完全一致；中途崩溃由幂等重放兜底。
     */
    public synchronized void compactNow() {
        snapshots.write(new SnapshotStore.SnapshotData(Instant.now().toEpochMilli(),
                committedOffset, sequence, acceptedTotal, rejectedDuplicateTotal,
                rejectedCapacityTotal, List.copyOf(tasks.values())));
        journal.reset();
        log.info("compaction done: snapshotTasks={}, committedOffset={}, journalReset",
                tasks.size(), committedOffset);
    }

    // ---------- 内部 ----------

    private void appendAndApply(JournalEvent event) {
        journal.append(event);
        apply(event);
        if (journal.sizeBytes() > compactThresholdBytes) {
            compactNow();
        }
    }

    /** 事件应用（幂等）：恢复重放与运行时追加共用同一套逻辑。 */
    private void apply(JournalEvent event) {
        switch (event) {
            case JournalEvent.TaskEnqueued e -> {
                // 压缩中途崩溃后旧 ENQ 会重放：已存在的任务不被覆盖，保证幂等
                if (!tasks.containsKey(e.id())) {
                    tasks.put(e.id(), new TaskSnapshot(e.id(), e.businessKey(), e.type(), e.payload(),
                            e.maxAttempts(), e.enqueuedAt(), TaskStatus.PENDING, 0,
                            e.enqueuedAt(), null, null, false, 0, 0));
                    acceptedTotal++;
                }
                sequence = Math.max(sequence, e.id());
            }
            case JournalEvent.TaskStatusChanged e -> {
                TaskSnapshot cur = tasks.get(e.id());
                if (cur == null) {
                    throw new RecoveryException("STATUS event for unknown task id=" + e.id()
                            + " (journal references a task that was never enqueued)");
                }
                // 只应用更新的版本：压缩崩溃后旧事件重放不会覆盖新状态
                if (e.version() >= cur.version()) {
                    tasks.put(e.id(), new TaskSnapshot(cur.id(), cur.businessKey(), cur.type(),
                            cur.payload(), cur.maxAttempts(), cur.enqueuedAt(), e.status(),
                            e.attempts(), e.nextVisibleAt(), e.lastFailureAt(),
                            e.lastErrorMessage(), e.sideEffectDone(), e.staleDeliveries(),
                            e.version()));
                }
            }
            case JournalEvent.OffsetCommitted e ->
                committedOffset = Math.max(committedOffset, e.committed());
            case JournalEvent.CountersUpdated e -> {
                acceptedTotal = Math.max(acceptedTotal, e.acceptedTotal());
                rejectedDuplicateTotal = Math.max(rejectedDuplicateTotal, e.rejectedDuplicateTotal());
                rejectedCapacityTotal = Math.max(rejectedCapacityTotal, e.rejectedCapacityTotal());
            }
        }
    }

    private void validateNoDuplicateBusinessKeys() {
        Map<String, Long> seen = new LinkedHashMap<>();
        for (TaskSnapshot t : tasks.values()) {
            Long prev = seen.put(t.businessKey(), t.id());
            if (prev != null) {
                throw new RecoveryException("duplicate businessKey '" + t.businessKey()
                        + "' for tasks " + prev + " and " + t.id()
                        + " (dedup index corrupted); refusing to start");
            }
        }
    }

    private static JournalEvent.TaskStatusChanged statusEvent(TaskSnapshot t) {
        return new JournalEvent.TaskStatusChanged(t.id(), t.status(), t.attempts(),
                t.nextVisibleAt(), t.lastFailureAt(), t.lastErrorMessage(),
                t.sideEffectDone(), t.staleDeliveries(), t.version());
    }

    private static TaskRecord toRecord(TaskSnapshot t) {
        return TaskRecord.restore(t.id(), t.businessKey(), t.type(), t.payload(),
                t.maxAttempts(), t.enqueuedAt(), t.status(), t.attempts(),
                t.nextVisibleAt(), t.lastFailureAt(), t.lastErrorMessage(),
                t.sideEffectDone(), t.staleDeliveries(), t.version());
    }

    // ---------- 测试与观测钩子 ----------

    /** 测试钩子：下一次位点提交不落盘，模拟“状态已写、位点未写”的崩溃点。 */
    public void simulateCrashBeforeNextOffsetWrite() {
        this.skipNextOffsetWrite = true;
    }

    public RecoveryReport lastRecovery() {
        return lastRecovery;
    }

    /** recover() 的产物（未调用 recover 前为空状态）。 */
    public RecoveredState recoveredState() {
        return recoveredState;
    }

    public Path dir() {
        return dir;
    }

    public Path journalFile() {
        return dir.resolve(JOURNAL_FILE);
    }

    public long journalSizeBytes() {
        return journal.sizeBytes();
    }

    @Override
    public synchronized void close() {
        journal.close();
    }
}
