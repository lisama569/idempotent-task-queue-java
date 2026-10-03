package com.github.lisama569.idempotenttaskqueuejava.persistence;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.zip.CRC32;

/**
 * 日志事件编解码。
 *
 * <p>帧格式：{@code [payloadLength:int][crc32:long][payload]}，CRC 只覆盖 payload。
 * 读帧时区分两类异常：</p>
 * <ul>
 *   <li><b>截断（torn tail）</b>：文件在帧中间结束 —— 上次写入一半被杀，安全截断；</li>
 *   <li><b>损坏（corrupt）</b>：帧完整但 CRC 不符或长度非法 —— 非追加写能产生的状态，拒绝启动。</li>
 * </ul>
 *
 * <p>事件类型：ENQUEUE / CLAIM / RETRY_WAITING / TERMINAL（终态+位点同帧，保证两者不矛盾）
 * / SIDE_EFFECT / REJECT（去重与容量拒绝计数）。除 SIDE_EFFECT/REJECT 外，事件携带
 * 应用后的记录版本号，重放时按“事件版本 &gt; 当前版本才应用”保证幂等（收拢崩溃后可安全重放）。</p>
 */
final class JournalCodec {
    static final int TYPE_ENQUEUE = 1;
    static final int TYPE_CLAIM = 2;
    static final int TYPE_RETRY_WAITING = 3;
    static final int TYPE_TERMINAL = 4;
    static final int TYPE_SIDE_EFFECT = 5;
    static final int TYPE_REJECT = 6;

    static final int REJECT_DUPLICATE = 0;
    static final int REJECT_CAPACITY = 1;

    static final int MAX_PAYLOAD_BYTES = 64 * 1024 * 1024;
    static final int FRAME_HEADER_BYTES = Integer.BYTES + Long.BYTES;

    private JournalCodec() {
    }

    /** 日志事件（扁平联合体，按 type 取用字段）。 */
    static final class JournalEvent {
        int type;
        long taskId;
        // ENQUEUE
        String businessKey;
        String taskType;
        String payload;
        int maxAttempts;
        Instant enqueuedAt;
        // CLAIM / RETRY_WAITING
        Instant nextVisibleAt;
        int staleDeliveries;
        // RETRY_WAITING / TERMINAL
        Instant lastFailureAt;
        String lastError;
        // TERMINAL
        TaskStatus terminalStatus;
        long committedOffset;
        // 幂等重放依据（记录变更后的版本号）
        long version;
        // REJECT
        int rejectKind;

        static JournalEvent enqueued(TaskRecord r) {
            JournalEvent e = new JournalEvent();
            e.type = TYPE_ENQUEUE;
            e.taskId = r.getId();
            e.businessKey = r.getBusinessKey();
            e.taskType = r.getType();
            e.payload = r.getPayload();
            e.maxAttempts = r.getMaxAttempts();
            e.enqueuedAt = r.getEnqueuedAt();
            e.nextVisibleAt = r.getNextVisibleAt();
            e.version = r.getVersion();
            return e;
        }

        static JournalEvent claimed(TaskRecord r) {
            JournalEvent e = new JournalEvent();
            e.type = TYPE_CLAIM;
            e.taskId = r.getId();
            e.nextVisibleAt = r.getNextVisibleAt();
            e.staleDeliveries = r.getStaleDeliveries();
            e.version = r.getVersion();
            return e;
        }

        static JournalEvent retryWaiting(TaskRecord r) {
            JournalEvent e = new JournalEvent();
            e.type = TYPE_RETRY_WAITING;
            e.taskId = r.getId();
            e.nextVisibleAt = r.getNextVisibleAt();
            e.lastFailureAt = r.getLastFailureAt();
            e.lastError = r.getLastErrorMessage();
            e.version = r.getVersion();
            return e;
        }

        static JournalEvent terminal(TaskRecord r, long committedOffset) {
            JournalEvent e = new JournalEvent();
            e.type = TYPE_TERMINAL;
            e.taskId = r.getId();
            e.terminalStatus = r.getStatus();
            e.lastError = r.getLastErrorMessage();
            e.lastFailureAt = r.getLastFailureAt();
            e.committedOffset = committedOffset;
            e.version = r.getVersion();
            return e;
        }

        static JournalEvent sideEffect(long taskId) {
            JournalEvent e = new JournalEvent();
            e.type = TYPE_SIDE_EFFECT;
            e.taskId = taskId;
            return e;
        }

        static JournalEvent reject(int rejectKind) {
            JournalEvent e = new JournalEvent();
            e.type = TYPE_REJECT;
            e.rejectKind = rejectKind;
            return e;
        }
    }

    /** 编码事件为完整帧（含长度前缀与 CRC）。 */
    static byte[] encodeFrame(JournalEvent e) {
        byte[] payload = encodePayload(e);
        CRC32 crc = new CRC32();
        crc.update(payload);
        ByteArrayOutputStream bos = new ByteArrayOutputStream(FRAME_HEADER_BYTES + payload.length);
        try {
            DataOutputStream out = new DataOutputStream(bos);
            out.writeInt(payload.length);
            out.writeLong(crc.getValue());
            out.write(payload);
            out.flush();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return bos.toByteArray();
    }

    /** 校验并解帧：返回 payload；帧不完整返回 null（torn tail）；CRC 不符抛异常（corrupt）。 */
    static byte[] tryDecodeFrame(byte[] buf, int offset, FrameResult result) {
        if (buf.length - offset < FRAME_HEADER_BYTES) {
            return null; // 连帧头都不完整：撕裂写
        }
        java.nio.ByteBuffer header = java.nio.ByteBuffer.wrap(buf, offset, FRAME_HEADER_BYTES);
        int len = header.getInt();
        long expectedCrc = header.getLong();
        if (len <= 0 || len > MAX_PAYLOAD_BYTES) {
            result.corrupt = true;
            result.detail = "illegal payload length " + len + " at offset " + offset;
            return null;
        }
        if (buf.length - offset - FRAME_HEADER_BYTES < len) {
            return null; // 帧体不完整：撕裂写
        }
        CRC32 crc = new CRC32();
        crc.update(buf, offset + FRAME_HEADER_BYTES, len);
        if (crc.getValue() != expectedCrc) {
            result.corrupt = true;
            result.detail = "crc mismatch at offset " + offset + ", payloadLength=" + len;
            return null;
        }
        result.frameBytes = FRAME_HEADER_BYTES + len;
        byte[] payload = new byte[len];
        System.arraycopy(buf, offset + FRAME_HEADER_BYTES, payload, 0, len);
        return payload;
    }

    /** 单次解帧的结果载体。 */
    static final class FrameResult {
        boolean corrupt;
        String detail;
        int frameBytes;
    }

    static JournalEvent decodeEvent(byte[] payload) {
        try {
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(payload));
            JournalEvent e = new JournalEvent();
            e.type = in.readByte();
            switch (e.type) {
                case TYPE_ENQUEUE -> {
                    e.taskId = in.readLong();
                    e.businessKey = readString(in);
                    e.taskType = readString(in);
                    e.payload = readString(in);
                    e.maxAttempts = in.readInt();
                    e.enqueuedAt = readInstant(in);
                    e.nextVisibleAt = readInstant(in);
                    e.version = in.readLong();
                }
                case TYPE_CLAIM -> {
                    e.taskId = in.readLong();
                    e.nextVisibleAt = readInstant(in);
                    e.staleDeliveries = in.readInt();
                    e.version = in.readLong();
                }
                case TYPE_RETRY_WAITING -> {
                    e.taskId = in.readLong();
                    e.nextVisibleAt = readInstant(in);
                    e.lastFailureAt = readInstant(in);
                    e.lastError = readString(in);
                    e.version = in.readLong();
                }
                case TYPE_TERMINAL -> {
                    e.taskId = in.readLong();
                    e.terminalStatus = in.readByte() == 0 ? TaskStatus.SUCCEEDED : TaskStatus.FAILED_PERMANENT;
                    e.lastError = readString(in);
                    e.lastFailureAt = readInstant(in);
                    e.committedOffset = in.readLong();
                    e.version = in.readLong();
                }
                case TYPE_SIDE_EFFECT -> e.taskId = in.readLong();
                case TYPE_REJECT -> e.rejectKind = in.readByte();
                default -> throw new RecoveryException("unknown journal event type " + e.type);
            }
            return e;
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static byte[] encodePayload(JournalEvent e) {
        ByteArrayOutputStream bos = new ByteArrayOutputStream(128);
        try {
            DataOutputStream out = new DataOutputStream(bos);
            out.writeByte(e.type);
            switch (e.type) {
                case TYPE_ENQUEUE -> {
                    out.writeLong(e.taskId);
                    writeString(out, e.businessKey);
                    writeString(out, e.taskType);
                    writeString(out, e.payload);
                    out.writeInt(e.maxAttempts);
                    writeInstant(out, e.enqueuedAt);
                    writeInstant(out, e.nextVisibleAt);
                    out.writeLong(e.version);
                }
                case TYPE_CLAIM -> {
                    out.writeLong(e.taskId);
                    writeInstant(out, e.nextVisibleAt);
                    out.writeInt(e.staleDeliveries);
                    out.writeLong(e.version);
                }
                case TYPE_RETRY_WAITING -> {
                    out.writeLong(e.taskId);
                    writeInstant(out, e.nextVisibleAt);
                    writeInstant(out, e.lastFailureAt);
                    writeString(out, e.lastError);
                    out.writeLong(e.version);
                }
                case TYPE_TERMINAL -> {
                    out.writeLong(e.taskId);
                    out.writeByte(e.terminalStatus == TaskStatus.SUCCEEDED ? 0 : 1);
                    writeString(out, e.lastError);
                    writeInstant(out, e.lastFailureAt);
                    out.writeLong(e.committedOffset);
                    out.writeLong(e.version);
                }
                case TYPE_SIDE_EFFECT -> out.writeLong(e.taskId);
                case TYPE_REJECT -> out.writeByte(e.rejectKind);
                default -> throw new IllegalArgumentException("unknown event type " + e.type);
            }
            out.flush();
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return bos.toByteArray();
    }

    private static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeInt(-1);
            return;
        }
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        out.writeInt(b.length);
        out.write(b);
    }

    private static String readString(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len < 0) {
            return null;
        }
        byte[] b = in.readNBytes(len);
        if (b.length != len) {
            throw new IOException("unexpected EOF reading string");
        }
        return new String(b, StandardCharsets.UTF_8);
    }

    private static void writeInstant(DataOutputStream out, Instant t) throws IOException {
        if (t == null) {
            out.writeLong(Long.MIN_VALUE);
            return;
        }
        out.writeLong(t.getEpochSecond());
        out.writeInt(t.getNano());
    }

    private static Instant readInstant(DataInputStream in) throws IOException {
        long epochSecond = in.readLong();
        if (epochSecond == Long.MIN_VALUE) {
            return null;
        }
        return Instant.ofEpochSecond(epochSecond, in.readInt());
    }
}
