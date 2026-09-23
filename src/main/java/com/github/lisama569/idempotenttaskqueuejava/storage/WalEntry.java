package com.github.lisama569.idempotenttaskqueuejava.storage;

import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;

/**
 * WAL 单条记录。KIND 取值：
 * <ul>
 *   <li>ENQUEUE：任务首次入队（携带完整 TaskRecord）</li>
 *   <li>UPDATE：任务状态迁移（携带完整 TaskRecord，回放以最后一条为准）</li>
 *   <li>OFFSET：已提交消费位点推进（value 为位点值）</li>
 * </ul>
 */
public record WalEntry(String kind, TaskRecord task, long value, long ts) {

    public static final String KIND_ENQUEUE = "ENQUEUE";
    public static final String KIND_UPDATE = "UPDATE";
    public static final String KIND_OFFSET = "OFFSET";
}
