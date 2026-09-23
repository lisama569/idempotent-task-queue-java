package com.github.lisama569.idempotenttaskqueuejava.storage;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

/**
 * 本地追加式 WAL 存储（无外部中间件依赖）。
 *
 * <p>每条提交以一行 JSON 写入，先 flush 再对 FileDescriptor 强制 fsync，
 * 保证“接受/状态迁移/位点推进”的判定一旦返回即落盘；
 * 重启时顺序回放，ENQUEUE/UPDATE 以同一任务最后一条记录为准，OFFSET 取最后一条。
 */
public class WalStore {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final Path logFile;
    private final java.io.FileOutputStream out;
    private final BufferedWriter writer;

    public WalStore(Path logFile) {
        this.logFile = logFile;
        try {
            if (logFile.getParent() != null) {
                Files.createDirectories(logFile.getParent());
            }
            this.out = new java.io.FileOutputStream(logFile.toFile(), true);
            this.writer = new BufferedWriter(new OutputStreamWriter(out, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("初始化 WAL 文件失败: " + logFile, e);
        }
    }

    /** 追加一条记录并强制落盘。 */
    public synchronized void append(WalEntry entry) {
        try {
            writer.write(MAPPER.writeValueAsString(entry));
            writer.newLine();
            writer.flush();
            out.getFD().sync();
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 追加失败", e);
        }
    }

    /** 启动时顺序回放全部日志。 */
    public synchronized List<WalEntry> replayAll() {
        List<WalEntry> entries = new ArrayList<>();
        if (!Files.exists(logFile)) {
            return entries;
        }
        try {
            for (String line : Files.readAllLines(logFile, StandardCharsets.UTF_8)) {
                if (line.isBlank()) {
                    continue;
                }
                JsonNode node = MAPPER.readTree(line);
                String kind = node.path("kind").asText();
                long value = node.path("value").asLong(0L);
                long ts = node.path("ts").asLong(0L);
                TaskRecord task = null;
                if (node.hasNonNull("task")) {
                    task = MAPPER.treeToValue(node.get("task"), TaskRecord.class);
                }
                entries.add(new WalEntry(kind, task, value, ts));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("WAL 回放失败: " + logFile, e);
        }
        return entries;
    }

    /** 供测试/运维查看日志文件位置。 */
    public Path getLogFile() {
        return logFile;
    }
}
