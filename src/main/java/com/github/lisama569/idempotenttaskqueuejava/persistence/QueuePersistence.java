package com.github.lisama569.idempotenttaskqueuejava.persistence;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * 本地持久化编排：追加式日志（WAL）+ 快照收拢，不依赖任何外部中间件。
 *
 * <p>崩溃一致性规则：</p>
 * <ul>
 *   <li>每条日志帧带长度+CRC32；帧尾撕裂（写一半被杀）→ 截断到最后一条完整帧，恢复为
 *       “已发生历史的一个前缀”，绝不会出现位点超前于任务状态等矛盾；</li>
 *   <li>终态迁移与位点推进写在<b>同一条</b> TERMINAL 帧里，两者要么同时生效要么同时不生效；</li>
 *   <li>帧完整但 CRC 不符 / 长度非法、或快照校验失败 → 抛 {@link RecoveryException} 拒绝启动，
 *       并说明卡在哪、判断依据；</li>
 *   <li>任务类事件携带记录版本号，重放按“事件版本 &gt; 当前版本才应用”幂等；
 *       拒绝（REJECT）事件携带全局单调 rejectSeq，快照拒绝计数之和即已覆盖序号上限，
 *       重放只应用超出上限的事件——因此收拢（先写快照再清日志）在 rename 与清日志之间
 *       被杀也不会把同一次拒绝计两次；</li>
 *   <li>旧格式（无 rejectSeq）的 REJECT 帧若与快照同时出现，覆盖关系无法判定，
 *       属于矛盾状态，拒绝启动并说明依据，绝不悄悄用可能重复计数的状态对外服务。</li>
 * </ul>
 *
 * <p>收拢（compact）：把当前全量状态写入 snapshot.tmp → fsync → 原子 rename 为 snapshot.dat →
 * 清空日志。rename 与清日志之间崩溃：快照与日志有重叠，靠版本号幂等重放消除。</p>
 */
public class QueuePersistence {
    private static final Logger log = LoggerFactory.getLogger(QueuePersistence.class);

    private static final String JOURNAL_FILE = "journal.log";
    private static final String SNAPSHOT_FILE = "snapshot.dat";
    private static final String SNAPSHOT_TMP_FILE = "snapshot.tmp";
    private static final int SNAPSHOT_MAGIC = 0x49545153; // "ITQS"
    private static final int SNAPSHOT_FORMAT_VERSION = 1;

    private final Path dir;
    private final Path journalPath;
    private final Path snapshotPath;
    private final FileChannel journal;
    private final RecoveredState recoveredState;
    /**
     * 已落盘的拒绝事件序号（全局单调、从 1 连续编号）。恢复时初始化为
     * “重放完成后实际生效的拒绝事件数”（= 两类拒绝计数之和），此后每追加一条 REJECT 帧 +1。
     * 它是 REJECT 重放幂等的依据：快照拒绝计数之和即快照已覆盖的 rejectSeq 上限。
     */
    private long rejectSeq;

    /** 恢复结果：交给 TaskStore/OffsetManager 一次性装载。 */
    public record RecoveredState(
            List<TaskRecord> tasks,
            long sequence,
            long acceptedTotal,
            long rejectedDuplicateTotal,
            long rejectedCapacityTotal,
            long committedOffset,
            RecoveryReport report) {
    }

    /** 收拢结果。 */
    public record CompactionReport(
            int tasksSnapshotted,
            long journalBytesBefore,
            long journalBytesAfter,
            long committedOffset) {
    }

    private QueuePersistence(Path dir, FileChannel journal, RecoveredState state) {
        this.dir = dir;
        this.journalPath = dir.resolve(JOURNAL_FILE);
        this.snapshotPath = dir.resolve(SNAPSHOT_FILE);
        this.journal = journal;
        this.recoveredState = state;
    }

    /**
     * 打开存储目录并执行恢复。目录不存在则创建（首次启动，空状态）。
     * 恢复失败（矛盾状态）抛 {@link RecoveryException}，拒绝启动。
     */
    public static QueuePersistence open(Path dir) {
        try {
            Files.createDirectories(dir);
            Path journalPath = dir.resolve(JOURNAL_FILE);
            Path snapshotPath = dir.resolve(SNAPSHOT_FILE);
            Path snapshotTmpPath = dir.resolve(SNAPSHOT_TMP_FILE);

            // 1) 未完成的收拢残留：tmp 快照直接丢弃（正式快照要么已原子替换、要么还是旧的，二者皆完整）
            if (Files.deleteIfExists(snapshotTmpPath)) {
                log.info("[RECOVERY] discarded incomplete snapshot tmp file (compaction interrupted), basis=tmp never renamed");
            }

            // 2) 装载快照（校验失败 → 拒绝启动）
            MutableState state = new MutableState();
            boolean snapshotLoaded = false;
            long snapshotOffset = 0L;
            int snapshotTaskCount = 0;
            if (Files.exists(snapshotPath)) {
                loadSnapshot(snapshotPath, state);
                snapshotLoaded = true;
                snapshotOffset = state.committedOffset;
                snapshotTaskCount = state.tasks.size();
                log.info("[RECOVERY] snapshot loaded: tasks={}, committedOffset={}, sequence={}",
                        snapshotTaskCount, snapshotOffset, state.sequence);
            }

            // 3) 重放日志；撕裂的帧尾截断，损坏的完整帧拒绝启动
            long replayed = 0L;
            long skipped = 0L;
            long truncatedAt = -1L;
            long tornBytes = 0L;
            long validLength = 0L;
            if (Files.exists(journalPath)) {
                byte[] bytes = Files.readAllBytes(journalPath);
                int offset = 0;
                while (offset < bytes.length) {
                    JournalCodec.FrameResult frame = new JournalCodec.FrameResult();
                    byte[] payload = JournalCodec.tryDecodeFrame(bytes, offset, frame);
                    if (payload == null) {
                        if (frame.corrupt) {
                            throw new RecoveryException(
                                    "journal corrupt at offset " + offset + " (" + frame.detail + "); "
                                            + "refusing to start: a complete-but-invalid frame cannot come from "
                                            + "an interrupted append, state would be ambiguous");
                        }
                        truncatedAt = offset;
                        tornBytes = bytes.length - offset;
                        log.warn("[RECOVERY] torn journal tail detected at offset={}, discarding {} bytes "
                                        + "(incomplete frame from interrupted write); basis=frame header/body truncated",
                                offset, tornBytes);
                        break;
                    }
                    if (apply(JournalCodec.decodeEvent(payload), state, snapshotLoaded)) {
                        replayed++;
                    } else {
                        skipped++;
                    }
                    offset += frame.frameBytes;
                    validLength = offset;
                }
            }
            if (skipped > 0) {
                log.info("[RECOVERY] skipped {} journal events already covered by snapshot "
                                + "(compaction was interrupted between snapshot rename and journal truncate); "
                                + "basis=rejectSeq <= snapshot reject counters / record version",
                        skipped);
            }

            // 4) 物理截断撕裂尾部，保证后续追加紧接最后一条完整帧
            FileChannel channel = FileChannel.open(journalPath,
                    StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            if (truncatedAt >= 0) {
                channel.truncate(validLength);
                channel.force(true);
            }
            channel.position(channel.size());

            RecoveryReport report = new RecoveryReport(
                    snapshotLoaded, snapshotOffset, snapshotTaskCount,
                    replayed, skipped, truncatedAt, tornBytes,
                    state.tasks.size(), state.committedOffset, state.sequence,
                    "snapshot(if valid) + ordered replay of crc-checked journal frames; "
                            + "torn tail truncated to last complete frame; "
                            + "terminal-state and offset committed in the same frame; "
                            + "task events idempotent by record version, reject events idempotent by "
                            + "global rejectSeq (snapshot reject counters = covered rejectSeq upper bound), "
                            + "so a compaction killed between snapshot rename and journal truncate "
                            + "never double-counts");
            log.info("[RECOVERY] done: tasksRecovered={}, committedOffset={}, sequence={}, "
                            + "journalFramesReplayed={}, tornTailTruncatedAt={}, basis={}",
                    report.tasksRecovered(), report.committedOffset(), report.sequence(),
                    report.journalRecordsReplayed(), report.journalTruncatedAtBytes(), report.basis());

            RecoveredState recovered = new RecoveredState(
                    new ArrayList<>(state.tasks.values()), state.sequence,
                    state.acceptedTotal, state.rejectedDuplicateTotal, state.rejectedCapacityTotal,
                    state.committedOffset, report);
            QueuePersistence persistence = new QueuePersistence(dir, channel, recovered);
            persistence.rejectSeq = state.rejectedDuplicateTotal + state.rejectedCapacityTotal;
            return persistence;
        } catch (IOException e) {
            throw new RecoveryException("io failure during recovery from dir " + dir + ": " + e.getMessage(), e);
        }
    }

    public RecoveredState recoveredState() {
        return recoveredState;
    }

    public RecoveryReport lastRecoveryReport() {
        return recoveredState.report();
    }

    // ---------- 追加（调用方持存储锁；本类内部再保证文件写串行） ----------

    public synchronized void onEnqueued(TaskRecord r) {
        append(JournalCodec.JournalEvent.enqueued(r));
    }

    public synchronized void onClaimed(TaskRecord r) {
        append(JournalCodec.JournalEvent.claimed(r));
    }

    public synchronized void onRetryWaiting(TaskRecord r) {
        append(JournalCodec.JournalEvent.retryWaiting(r));
    }

    /** 终态与位点必须同帧：这是“位点不超前于任务状态”的持久化依据。 */
    public synchronized void onTerminal(TaskRecord r, long committedOffset) {
        append(JournalCodec.JournalEvent.terminal(r, committedOffset));
    }

    public synchronized void onSideEffect(long taskId) {
        append(JournalCodec.JournalEvent.sideEffect(taskId));
    }

    public synchronized void onRejectedDuplicate() {
        append(JournalCodec.JournalEvent.reject(JournalCodec.REJECT_DUPLICATE, ++rejectSeq));
    }

    public synchronized void onRejectedCapacity() {
        append(JournalCodec.JournalEvent.reject(JournalCodec.REJECT_CAPACITY, ++rejectSeq));
    }

    public synchronized long journalSizeBytes() {
        try {
            return journal.position();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void append(JournalCodec.JournalEvent event) {
        try {
            journal.write(ByteBuffer.wrap(JournalCodec.encodeFrame(event)));
            journal.force(true);
        } catch (IOException e) {
            throw new UncheckedIOException("journal append failed: " + e.getMessage(), e);
        }
    }

    // ---------- 收拢 ----------

    /**
     * 把当前全量状态收拢为快照并清空日志。调用方须持有存储锁，保证快照内容自洽。
     * 中途崩溃安全：快照已原子替换而日志未清 → 重叠事件按版本号幂等跳过。
     */
    public synchronized CompactionReport compact(List<TaskRecord> records, long sequence,
                                                 long acceptedTotal, long rejectedDuplicateTotal,
                                                 long rejectedCapacityTotal, long committedOffset) {
        long journalBefore = journalSizeBytes();
        try {
            Path tmp = dir.resolve(SNAPSHOT_TMP_FILE);
            byte[] payload = encodeSnapshotPayload(records, sequence, acceptedTotal,
                    rejectedDuplicateTotal, rejectedCapacityTotal, committedOffset);
            CRC32 crc = new CRC32();
            crc.update(payload);
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bos);
            out.writeInt(SNAPSHOT_MAGIC);
            out.writeInt(SNAPSHOT_FORMAT_VERSION);
            out.writeInt(payload.length);
            out.writeLong(crc.getValue());
            out.write(payload);
            out.flush();
            Files.write(tmp, bos.toByteArray(),
                    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
            // 先让 tmp 落盘，再原子替换正式快照
            try (FileChannel tmpChannel = FileChannel.open(tmp, StandardOpenOption.WRITE)) {
                tmpChannel.force(true);
            }
            Files.move(tmp, snapshotPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            // 快照已生效：清空日志（崩溃则靠版本号幂等重放重叠部分）
            journal.truncate(0);
            journal.force(true);
            journal.position(0);
        } catch (IOException e) {
            throw new UncheckedIOException("compaction failed: " + e.getMessage(), e);
        }
        CompactionReport report = new CompactionReport(records.size(), journalBefore, 0L, committedOffset);
        log.info("[COMPACT] snapshot written: tasks={}, committedOffset={}, journalBytes {} -> 0",
                report.tasksSnapshotted(), committedOffset, journalBefore);
        return report;
    }

    public synchronized void close() {
        try {
            journal.close();
        } catch (IOException e) {
            log.warn("failed to close journal: {}", e.getMessage());
        }
    }

    // ---------- 快照编解码与重放 ----------

    private static byte[] encodeSnapshotPayload(List<TaskRecord> records, long sequence,
                                                long acceptedTotal, long rejectedDuplicateTotal,
                                                long rejectedCapacityTotal, long committedOffset) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(bos);
        out.writeLong(sequence);
        out.writeLong(acceptedTotal);
        out.writeLong(rejectedDuplicateTotal);
        out.writeLong(rejectedCapacityTotal);
        out.writeLong(committedOffset);
        out.writeInt(records.size());
        for (TaskRecord r : records) {
            out.writeLong(r.getId());
            writeStr(out, r.getBusinessKey());
            writeStr(out, r.getType());
            writeStr(out, r.getPayload());
            out.writeByte(statusCode(r.getStatus()));
            out.writeInt(r.getAttempts());
            out.writeInt(r.getMaxAttempts());
            writeInstant(out, r.getEnqueuedAt());
            writeInstant(out, r.getNextVisibleAt());
            writeInstant(out, r.getLastFailureAt());
            writeStr(out, r.getLastErrorMessage());
            out.writeByte(r.isSideEffectDone() ? 1 : 0);
            out.writeInt(r.getStaleDeliveries());
            out.writeLong(r.getVersion());
        }
        out.flush();
        return bos.toByteArray();
    }

    private static void loadSnapshot(Path snapshotPath, MutableState state) throws IOException {
        byte[] bytes = Files.readAllBytes(snapshotPath);
        if (bytes.length < Integer.BYTES * 3 + Long.BYTES) {
            throw new RecoveryException("snapshot too short (" + bytes.length
                    + " bytes); refusing to start: snapshot is the only state source after compaction");
        }
        ByteBuffer header = ByteBuffer.wrap(bytes);
        int magic = header.getInt();
        int version = header.getInt();
        int len = header.getInt();
        long expectedCrc = header.getLong();
        if (magic != SNAPSHOT_MAGIC) {
            throw new RecoveryException("snapshot magic mismatch: " + Integer.toHexString(magic)
                    + "; refusing to start (not a queue snapshot or corrupted header)");
        }
        if (version != SNAPSHOT_FORMAT_VERSION) {
            throw new RecoveryException("snapshot format version " + version + " not supported (expected "
                    + SNAPSHOT_FORMAT_VERSION + "); refusing to start");
        }
        if (len <= 0 || bytes.length != Integer.BYTES * 3 + Long.BYTES + len) {
            throw new RecoveryException("snapshot length field inconsistent: declared=" + len
                    + ", actual=" + (bytes.length - Integer.BYTES * 3 - Long.BYTES)
                    + "; refusing to start");
        }
        CRC32 crc = new CRC32();
        crc.update(bytes, Integer.BYTES * 3 + Long.BYTES, len);
        if (crc.getValue() != expectedCrc) {
            throw new RecoveryException("snapshot crc mismatch; refusing to start: "
                    + "state would be ambiguous");
        }
        try {
            java.io.DataInputStream in = new java.io.DataInputStream(
                    new java.io.ByteArrayInputStream(bytes, Integer.BYTES * 3 + Long.BYTES, len));
            state.sequence = in.readLong();
            state.acceptedTotal = in.readLong();
            state.rejectedDuplicateTotal = in.readLong();
            state.rejectedCapacityTotal = in.readLong();
            state.committedOffset = in.readLong();
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                long id = in.readLong();
                String businessKey = readStr(in);
                String type = readStr(in);
                String payload = readStr(in);
                TaskStatus status = statusFromCode(in.readByte());
                int attempts = in.readInt();
                int maxAttempts = in.readInt();
                Instant enqueuedAt = readInstant(in);
                Instant nextVisibleAt = readInstant(in);
                Instant lastFailureAt = readInstant(in);
                String lastError = readStr(in);
                boolean sideEffectDone = in.readByte() == 1;
                int staleDeliveries = in.readInt();
                long recordVersion = in.readLong();
                state.tasks.put(id, TaskRecord.restore(id, businessKey, type, payload, maxAttempts,
                        enqueuedAt, status, attempts, nextVisibleAt, lastFailureAt, lastError,
                        sideEffectDone, staleDeliveries, recordVersion));
            }
        } catch (IOException e) {
            throw new RecoveryException("snapshot payload undecodable: " + e.getMessage()
                    + "; refusing to start", e);
        }
    }

    /**
     * 应用一条日志事件；返回 false 表示该事件已被快照覆盖而跳过（收拢在“快照已替换、
     * 日志未清”之间被杀时，快照与日志存在重叠）。
     *
     * <p>幂等依据：任务类事件按“事件版本 &gt; 当前版本才应用”；REJECT 事件不依附于任务记录，
     * 按全局单调 rejectSeq 判定——快照中的拒绝计数之和就是快照已覆盖的 rejectSeq 上限，
     * 序号不超过该上限的事件一律跳过，同一次拒绝绝不重复计数。</p>
     */
    private static boolean apply(JournalCodec.JournalEvent e, MutableState state, boolean snapshotLoaded) {
        switch (e.type) {
            case JournalCodec.TYPE_ENQUEUE -> {
                if (state.tasks.containsKey(e.taskId)) {
                    return false; // 快照已覆盖
                }
                state.tasks.put(e.taskId, TaskRecord.restore(e.taskId, e.businessKey, e.taskType,
                        e.payload, e.maxAttempts, e.enqueuedAt, TaskStatus.PENDING, 0,
                        e.nextVisibleAt, null, null, false, 0, e.version));
                state.sequence = Math.max(state.sequence, e.taskId);
                state.acceptedTotal++;
            }
            case JournalCodec.TYPE_CLAIM -> {
                TaskRecord r = state.tasks.get(e.taskId);
                if (r != null && e.version > r.getVersion()) {
                    r.markDelivering(e.nextVisibleAt);
                    while (r.getStaleDeliveries() < e.staleDeliveries) {
                        r.countStaleDelivery();
                    }
                }
            }
            case JournalCodec.TYPE_RETRY_WAITING -> {
                TaskRecord r = state.tasks.get(e.taskId);
                if (r != null && e.version > r.getVersion()) {
                    r.markRetryWaiting(e.nextVisibleAt, e.lastError,
                            e.lastFailureAt != null ? e.lastFailureAt : e.nextVisibleAt);
                }
            }
            case JournalCodec.TYPE_TERMINAL -> {
                TaskRecord r = state.tasks.get(e.taskId);
                if (r != null && e.version > r.getVersion()) {
                    if (e.terminalStatus == TaskStatus.SUCCEEDED) {
                        r.markSucceeded(e.lastFailureAt != null ? e.lastFailureAt : Instant.EPOCH);
                    } else {
                        r.markFailedPermanent(e.lastError,
                                e.lastFailureAt != null ? e.lastFailureAt : Instant.EPOCH);
                    }
                }
                // 位点与终态同帧：只前进不回退
                state.committedOffset = Math.max(state.committedOffset, e.committedOffset);
            }
            case JournalCodec.TYPE_SIDE_EFFECT -> {
                TaskRecord r = state.tasks.get(e.taskId);
                if (r != null) {
                    r.markSideEffectDone();
                }
            }
            case JournalCodec.TYPE_REJECT -> {
                // 已生效的拒绝事件数 = 两类计数之和；rejectSeq 从 1 连续编号，
                // 因此“快照已覆盖的上限”就是当前计数之和。
                long appliedRejects = state.rejectedDuplicateTotal + state.rejectedCapacityTotal;
                if (e.rejectSeq == 0L) {
                    // 旧格式帧没有序号：无快照时不存在被重复计数的来源，可安全应用；
                    // 有快照时无法判定该帧是否已计入快照计数 —— 矛盾状态，拒绝启动并说明依据。
                    if (snapshotLoaded) {
                        throw new RecoveryException(
                                "journal contains a legacy REJECT event without rejectSeq while a snapshot "
                                        + "exists; cannot determine whether this rejection is already counted "
                                        + "in the snapshot (reject counters would be ambiguous); refusing to start. "
                                        + "Basis: snapshot reject totals="
                                        + appliedRejects + " but the event carries no sequence to compare against");
                    }
                } else if (e.rejectSeq <= appliedRejects) {
                    return false; // 快照已覆盖：收拢在 rename 与清日志之间被杀，跳过防重复计数
                } else if (e.rejectSeq > appliedRejects + 1) {
                    throw new RecoveryException(
                            "reject sequence gap: expected " + (appliedRejects + 1) + " but found "
                                    + e.rejectSeq + "; journal is missing reject events without a torn tail, "
                                    + "state would be ambiguous; refusing to start");
                }
                if (e.rejectKind == JournalCodec.REJECT_DUPLICATE) {
                    state.rejectedDuplicateTotal++;
                } else {
                    state.rejectedCapacityTotal++;
                }
            }
            default -> throw new RecoveryException("unknown journal event type " + e.type);
        }
        return true;
    }

    private static int statusCode(TaskStatus s) {
        return switch (s) {
            case PENDING -> 0;
            case DELIVERING -> 1;
            case RETRY_WAITING -> 2;
            case SUCCEEDED -> 3;
            case FAILED_PERMANENT -> 4;
        };
    }

    private static TaskStatus statusFromCode(int code) {
        return switch (code) {
            case 0 -> TaskStatus.PENDING;
            case 1 -> TaskStatus.DELIVERING;
            case 2 -> TaskStatus.RETRY_WAITING;
            case 3 -> TaskStatus.SUCCEEDED;
            case 4 -> TaskStatus.FAILED_PERMANENT;
            default -> throw new RecoveryException("unknown task status code " + code + " in snapshot");
        };
    }

    private static void writeStr(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        byte[] b = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readStr(java.io.DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            return null;
        }
        byte[] b = in.readNBytes(len);
        if (b.length != len) {
            throw new IOException("unexpected EOF reading string");
        }
        return new String(b, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static void writeInstant(DataOutputStream out, Instant t) throws IOException {
        if (t == null) {
            out.writeLong(Long.MIN_VALUE);
            return;
        }
        out.writeLong(t.getEpochSecond());
        out.writeInt(t.getNano());
    }

    private static Instant readInstant(java.io.DataInputStream in) throws IOException {
        long epochSecond = in.readLong();
        if (epochSecond == Long.MIN_VALUE) {
            return null;
        }
        return Instant.ofEpochSecond(epochSecond, in.readInt());
    }

    /** 重放过程中的可变累积态。 */
    private static final class MutableState {
        final Map<Long, TaskRecord> tasks = new LinkedHashMap<>();
        long sequence;
        long acceptedTotal;
        long rejectedDuplicateTotal;
        long rejectedCapacityTotal;
        long committedOffset;
    }

}
