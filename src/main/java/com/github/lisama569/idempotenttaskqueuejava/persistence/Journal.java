package com.github.lisama569.idempotenttaskqueuejava.persistence;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Closeable;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * 追加式本地日志（JSON Lines + 每行 CRC32 校验）。
 *
 * <p>崩溃安全约定：</p>
 * <ul>
 *   <li>每次 append 后 {@code getFD().sync()}，返回即表示已落盘；</li>
 *   <li>恢复时逐行校验 CRC，遇到不可解析/校验失败的行视为“撕裂尾部”，
 *       将文件截断到最后一条完整记录之后，并记录截断字节数；</li>
 *   <li>事件应用幂等，因此压缩（快照 + reset）中途崩溃后重放旧事件不会产生矛盾。</li>
 * </ul>
 */
public class Journal implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(Journal.class);

    private final Path file;
    private RandomAccessFile writer;
    private long lastTornTailBytes;
    private long lastReplayedBytes;

    public Journal(Path file) {
        this.file = file;
    }

    /** 追加一条事件并落盘（fsync）。 */
    public synchronized void append(JournalEvent event) {
        ensureWriter();
        try {
            writer.write((JsonCodec.eventToLine(event) + "\n").getBytes(StandardCharsets.UTF_8));
            writer.getFD().sync();
        } catch (IOException e) {
            throw new RecoveryException("journal append failed: " + file, e);
        }
    }

    /**
     * 从头重放全部有效事件；遇到撕裂/损坏尾部时截断文件并继续启动。
     * 截断本身也落盘，保证下次启动看到的是干净尾部。
     */
    public synchronized List<JournalEvent> replay() {
        lastTornTailBytes = 0;
        lastReplayedBytes = 0;
        List<JournalEvent> events = new ArrayList<>();
        if (!Files.exists(file)) {
            return events;
        }
        try (RandomAccessFile raf = new RandomAccessFile(file.toFile(), "rw")) {
            long goodEnd = 0L;
            String line;
            while ((line = raf.readLine()) != null) {
                long lineEnd = raf.getFilePointer();
                if (line.isBlank()) {
                    // 空行只可能来自撕裂写（正常写入不会产出空行）
                    lastTornTailBytes = raf.length() - goodEnd;
                    break;
                }
                // readLine 按 ISO-8859-1 读字节，原样转回 UTF-8 字节再解码
                String utf8 = new String(line.getBytes(StandardCharsets.ISO_8859_1), StandardCharsets.UTF_8);
                try {
                    events.add(JsonCodec.eventFromLine(utf8));
                    goodEnd = lineEnd;
                } catch (RecoveryException e) {
                    lastTornTailBytes = raf.length() - goodEnd;
                    log.warn("journal torn tail detected at offset={}, truncatedBytes={}, reason={}",
                            goodEnd, lastTornTailBytes, e.getMessage());
                    break;
                }
            }
            if (lastTornTailBytes > 0) {
                raf.setLength(goodEnd);
                raf.getFD().sync();
            }
            lastReplayedBytes = goodEnd;
        } catch (IOException e) {
            throw new RecoveryException("journal replay failed: " + file, e);
        }
        return events;
    }

    /** 上次 replay 截掉的撕裂尾部长度（字节）。 */
    public long lastTornTailBytes() {
        return lastTornTailBytes;
    }

    /** 上次 replay 读到的事件字节数（不含被截断部分）。 */
    public long lastReplayedBytes() {
        return lastReplayedBytes;
    }

    /** 当前日志大小（字节）。 */
    public synchronized long sizeBytes() {
        try {
            return Files.exists(file) ? Files.size(file) : 0L;
        } catch (IOException e) {
            throw new RecoveryException("journal size check failed: " + file, e);
        }
    }

    /** 压缩后重置日志为空文件（先截断再 fsync）。 */
    public synchronized void reset() {
        ensureWriter();
        try {
            writer.setLength(0L);
            writer.seek(0L);
            writer.getFD().sync();
        } catch (IOException e) {
            throw new RecoveryException("journal reset failed: " + file, e);
        }
    }

    @Override
    public synchronized void close() {
        if (writer != null) {
            try {
                writer.getFD().sync();
                writer.close();
            } catch (IOException e) {
                log.warn("journal close failed: {}", file, e);
            } finally {
                writer = null;
            }
        }
    }

    private void ensureWriter() {
        if (writer == null) {
            try {
                if (file.getParent() != null) {
                    Files.createDirectories(file.getParent());
                }
                writer = new RandomAccessFile(file.toFile(), "rw");
                writer.seek(writer.length());
            } catch (IOException e) {
                throw new RecoveryException("journal open failed: " + file, e);
            }
        }
    }
}
