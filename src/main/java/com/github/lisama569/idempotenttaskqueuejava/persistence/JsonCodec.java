package com.github.lisama569.idempotenttaskqueuejava.persistence;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import java.time.Instant;
import java.util.zip.CRC32;

/**
 * 持久化 JSON 编解码：每行一个 JSON 对象 {@code {"crc":N,"type":T,"data":{...}}}，
 * crc 覆盖 type 与 data 的规范化串，用于识别撕裂写/损坏行。
 * Instant 一律序列化为 epochMilli（可空），避免额外模块依赖且保证字节级稳定。
 */
final class JsonCodec {

    static final ObjectMapper MAPPER = new ObjectMapper();

    private JsonCodec() {
    }

    // ---------- 事件 ----------

    static String eventToLine(JournalEvent event) {
        ObjectNode data = MAPPER.createObjectNode();
        String type;
        switch (event) {
            case JournalEvent.TaskEnqueued e -> {
                type = "ENQ";
                data.put("id", e.id());
                data.put("businessKey", e.businessKey());
                data.put("type", e.type());
                data.put("payload", e.payload());
                data.put("maxAttempts", e.maxAttempts());
                putInstant(data, "enqueuedAt", e.enqueuedAt());
            }
            case JournalEvent.TaskStatusChanged e -> {
                type = "STATUS";
                data.put("id", e.id());
                data.put("status", e.status().name());
                data.put("attempts", e.attempts());
                putInstant(data, "nextVisibleAt", e.nextVisibleAt());
                putInstant(data, "lastFailureAt", e.lastFailureAt());
                if (e.lastErrorMessage() == null) {
                    data.putNull("lastErrorMessage");
                } else {
                    data.put("lastErrorMessage", e.lastErrorMessage());
                }
                data.put("sideEffectDone", e.sideEffectDone());
                data.put("staleDeliveries", e.staleDeliveries());
                data.put("version", e.version());
            }
            case JournalEvent.OffsetCommitted e -> {
                type = "OFFSET";
                data.put("committed", e.committed());
            }
            case JournalEvent.CountersUpdated e -> {
                type = "COUNTERS";
                data.put("acceptedTotal", e.acceptedTotal());
                data.put("rejectedDuplicateTotal", e.rejectedDuplicateTotal());
                data.put("rejectedCapacityTotal", e.rejectedCapacityTotal());
            }
        }
        return envelope(type, data);
    }

    static JournalEvent eventFromLine(String line) {
        ObjectNode data = parseEnvelope(line);
        String type = envelopeType(line);
        return switch (type) {
            case "ENQ" -> new JournalEvent.TaskEnqueued(
                    data.get("id").asLong(),
                    data.get("businessKey").asText(),
                    data.get("type").asText(),
                    data.get("payload").asText(),
                    data.get("maxAttempts").asInt(),
                    getInstant(data, "enqueuedAt"));
            case "STATUS" -> new JournalEvent.TaskStatusChanged(
                    data.get("id").asLong(),
                    TaskStatus.valueOf(data.get("status").asText()),
                    data.get("attempts").asInt(),
                    getInstant(data, "nextVisibleAt"),
                    getInstant(data, "lastFailureAt"),
                    data.hasNonNull("lastErrorMessage") ? data.get("lastErrorMessage").asText() : null,
                    data.get("sideEffectDone").asBoolean(),
                    data.get("staleDeliveries").asInt(),
                    data.get("version").asLong());
            case "OFFSET" -> new JournalEvent.OffsetCommitted(data.get("committed").asLong());
            case "COUNTERS" -> new JournalEvent.CountersUpdated(
                    data.get("acceptedTotal").asLong(),
                    data.get("rejectedDuplicateTotal").asLong(),
                    data.get("rejectedCapacityTotal").asLong());
            default -> throw new RecoveryException("unknown journal event type: " + type);
        };
    }

    // ---------- 快照任务行 ----------

    static String taskToLine(TaskSnapshot t) {
        ObjectNode data = MAPPER.createObjectNode();
        data.put("id", t.id());
        data.put("businessKey", t.businessKey());
        data.put("type", t.type());
        data.put("payload", t.payload());
        data.put("maxAttempts", t.maxAttempts());
        putInstant(data, "enqueuedAt", t.enqueuedAt());
        data.put("status", t.status().name());
        data.put("attempts", t.attempts());
        putInstant(data, "nextVisibleAt", t.nextVisibleAt());
        putInstant(data, "lastFailureAt", t.lastFailureAt());
        if (t.lastErrorMessage() == null) {
            data.putNull("lastErrorMessage");
        } else {
            data.put("lastErrorMessage", t.lastErrorMessage());
        }
        data.put("sideEffectDone", t.sideEffectDone());
        data.put("staleDeliveries", t.staleDeliveries());
        data.put("version", t.version());
        return envelope("TASK", data);
    }

    static TaskSnapshot taskFromLine(String line) {
        ObjectNode data = parseEnvelope(line);
        if (!"TASK".equals(envelopeType(line))) {
            throw new RecoveryException("expected TASK line but got: " + envelopeType(line));
        }
        return new TaskSnapshot(
                data.get("id").asLong(),
                data.get("businessKey").asText(),
                data.get("type").asText(),
                data.get("payload").asText(),
                data.get("maxAttempts").asInt(),
                getInstant(data, "enqueuedAt"),
                TaskStatus.valueOf(data.get("status").asText()),
                data.get("attempts").asInt(),
                getInstant(data, "nextVisibleAt"),
                getInstant(data, "lastFailureAt"),
                data.hasNonNull("lastErrorMessage") ? data.get("lastErrorMessage").asText() : null,
                data.get("sideEffectDone").asBoolean(),
                data.get("staleDeliveries").asInt(),
                data.get("version").asLong());
    }

    // ---------- 信封与校验 ----------

    /** 构造带 CRC 的信封行；crc 覆盖 type 与 data 的串。 */
    static String envelope(String type, ObjectNode data) {
        String dataJson = data.toString();
        ObjectNode env = MAPPER.createObjectNode();
        env.put("crc", crc(type + "|" + dataJson));
        env.put("type", type);
        env.put("data", data);
        return env.toString();
    }

    /** 解析并校验信封行，返回 data 节点；损坏时抛 RecoveryException。 */
    static ObjectNode parseEnvelope(String line) {
        try {
            JsonNode env = MAPPER.readTree(line);
            long expected = env.get("crc").asLong();
            String type = env.get("type").asText();
            JsonNode data = env.get("data");
            long actual = crc(type + "|" + data.toString());
            if (expected != actual) {
                throw new RecoveryException("crc mismatch: expected=" + expected + ", actual=" + actual);
            }
            return (ObjectNode) data;
        } catch (RecoveryException e) {
            throw e;
        } catch (Exception e) {
            throw new RecoveryException("unparseable line: " + abbreviate(line), e);
        }
    }

    static String envelopeType(String line) {
        try {
            return MAPPER.readTree(line).get("type").asText();
        } catch (Exception e) {
            throw new RecoveryException("unparseable line: " + abbreviate(line), e);
        }
    }

    static long crc(String s) {
        CRC32 c = new CRC32();
        c.update(s.getBytes(java.nio.charset.StandardCharsets.UTF_8));
        return c.getValue();
    }

    private static void putInstant(ObjectNode node, String field, Instant value) {
        if (value == null) {
            node.putNull(field);
        } else {
            // ISO-8601 字符串：保留纳秒精度，保证重启前后查询结果逐字段一致
            node.put(field, value.toString());
        }
    }

    private static Instant getInstant(ObjectNode node, String field) {
        return node.hasNonNull(field) ? Instant.parse(node.get(field).asText()) : null;
    }

    private static String abbreviate(String s) {
        return s.length() <= 80 ? s : s.substring(0, 80) + "...";
    }
}
