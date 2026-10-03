package com.github.lisama569.idempotenttaskqueuejava.persistence;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * 快照读写：先写 {@code snapshot.jsonl.tmp} + fsync，再原子 rename 为
 * {@code snapshot.jsonl}，最后 fsync 目录。因此快照文件要么是旧的完整版本、
 * 要么是新的完整版本；压缩中途被杀掉只会留下可忽略的 .tmp 残文件。
 *
 * <p>文件结构（每行均为带 CRC 的信封）：HEADER → TASK* → END（含任务数校验）。</p>
 */
public class SnapshotStore {
    private static final Logger log = LoggerFactory.getLogger(SnapshotStore.class);

    public static final String SNAPSHOT_FILE = "snapshot.jsonl";
    public static final String SNAPSHOT_TMP_FILE = "snapshot.jsonl.tmp";

    /** 快照内容。 */
    public record SnapshotData(long createdAtEpochMillis, long committedOffset, long sequence,
                               long acceptedTotal, long rejectedDuplicateTotal,
                               long rejectedCapacityTotal, List<TaskSnapshot> tasks) {
    }

    private final Path dir;

    public SnapshotStore(Path dir) {
        this.dir = dir;
    }

    /** 原子写入快照。 */
    public void write(SnapshotData data) {
        try {
            Files.createDirectories(dir);
            Path tmp = dir.resolve(SNAPSHOT_TMP_FILE);
            StringBuilder sb = new StringBuilder();
            ObjectNode header = JsonCodec.MAPPER.createObjectNode();
            header.put("version", 1);
            header.put("createdAt", data.createdAtEpochMillis());
            header.put("committedOffset", data.committedOffset());
            header.put("sequence", data.sequence());
            header.put("acceptedTotal", data.acceptedTotal());
            header.put("rejectedDuplicateTotal", data.rejectedDuplicateTotal());
            header.put("rejectedCapacityTotal", data.rejectedCapacityTotal());
            header.put("taskCount", data.tasks().size());
            sb.append(JsonCodec.envelope("HEADER", header)).append('\n');
            for (TaskSnapshot t : data.tasks()) {
                sb.append(JsonCodec.taskToLine(t)).append('\n');
            }
            ObjectNode end = JsonCodec.MAPPER.createObjectNode();
            end.put("taskCount", data.tasks().size());
            sb.append(JsonCodec.envelope("END", end)).append('\n');

            try (RandomAccessFile raf = new RandomAccessFile(tmp.toFile(), "rw")) {
                raf.setLength(0L);
                raf.write(sb.toString().getBytes(StandardCharsets.UTF_8));
                raf.getFD().sync();
            }
            Files.move(tmp, dir.resolve(SNAPSHOT_FILE), StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
            fsyncDir();
            log.info("snapshot written: tasks={}, committedOffset={}, sequence={}",
                    data.tasks().size(), data.committedOffset(), data.sequence());
        } catch (IOException e) {
            throw new RecoveryException("snapshot write failed in " + dir, e);
        }
    }

    /**
     * 读取并校验快照；不存在返回空。
     * 存在但损坏（缺 END、行 CRC 失败、任务数不符）时抛 RecoveryException 拒绝启动——
     * 原子 rename 保证这种情况只可能来自磁盘层损坏，不能悄悄忽略。
     */
    public Optional<SnapshotData> read() {
        Path snapshot = dir.resolve(SNAPSHOT_FILE);
        Path staleTmp = dir.resolve(SNAPSHOT_TMP_FILE);
        try {
            if (Files.exists(staleTmp)) {
                // 上次压缩在 rename 前被杀：残文件不含已提交语义，安全清理
                Files.delete(staleTmp);
                log.info("discarded stale snapshot tmp file (crash before atomic rename): {}", staleTmp);
            }
            if (!Files.exists(snapshot)) {
                return Optional.empty();
            }
            List<String> lines = Files.readAllLines(snapshot, StandardCharsets.UTF_8);
            if (lines.size() < 2) {
                throw new RecoveryException("snapshot too short (" + lines.size() + " lines): " + snapshot);
            }
            ObjectNode header = JsonCodec.parseEnvelope(lines.getFirst());
            if (!"HEADER".equals(JsonCodec.envelopeType(lines.getFirst()))) {
                throw new RecoveryException("snapshot missing HEADER line: " + snapshot);
            }
            ObjectNode end = JsonCodec.parseEnvelope(lines.getLast());
            if (!"END".equals(JsonCodec.envelopeType(lines.getLast()))) {
                throw new RecoveryException("snapshot missing END line (incomplete write?): " + snapshot);
            }
            int expectedTasks = header.get("taskCount").asInt();
            if (end.get("taskCount").asInt() != expectedTasks) {
                throw new RecoveryException("snapshot END taskCount mismatch: header=" + expectedTasks
                        + ", end=" + end.get("taskCount").asInt());
            }
            List<TaskSnapshot> tasks = new ArrayList<>(expectedTasks);
            for (int i = 1; i < lines.size() - 1; i++) {
                tasks.add(JsonCodec.taskFromLine(lines.get(i)));
            }
            if (tasks.size() != expectedTasks) {
                throw new RecoveryException("snapshot task count mismatch: header=" + expectedTasks
                        + ", actual=" + tasks.size());
            }
            return Optional.of(new SnapshotData(
                    header.get("createdAt").asLong(),
                    header.get("committedOffset").asLong(),
                    header.get("sequence").asLong(),
                    header.get("acceptedTotal").asLong(),
                    header.get("rejectedDuplicateTotal").asLong(),
                    header.get("rejectedCapacityTotal").asLong(),
                    tasks));
        } catch (IOException e) {
            throw new RecoveryException("snapshot read failed: " + snapshot, e);
        }
    }

    private void fsyncDir() {
        try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(dir)) {
            ch.force(true);
        } catch (IOException e) {
            log.warn("directory fsync failed (snapshot rename durability weakened): {}", dir, e);
        }
    }
}
