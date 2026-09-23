package com.github.lisama569.idempotenttaskqueuejava.model;

import java.util.List;

/**
 * 队列可查询快照：用于状态查询与测试断言、日志打印。
 */
public record QueueSnapshot(
        long committedOffset,
        long nextSeq,
        int capacity,
        int activeCount,
        long enqueuedTotal,
        long rejectedTotal,
        List<TaskRecord> tasks
) {
}
