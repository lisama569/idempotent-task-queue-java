package com.github.lisama569.idempotenttaskqueuejava.persistence;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import java.util.List;

/**
 * 恢复产物：用于重建 TaskStore / OffsetManager 的全部状态 + 诊断报告。
 */
public record RecoveredState(
        List<TaskRecord> records,
        long sequence,
        long committedOffset,
        long acceptedTotal,
        long rejectedDuplicateTotal,
        long rejectedCapacityTotal,
        RecoveryReport report
) {
    public static RecoveredState empty() {
        return new RecoveredState(List.of(), 0L, 0L, 0L, 0L, 0L, null);
    }
}
