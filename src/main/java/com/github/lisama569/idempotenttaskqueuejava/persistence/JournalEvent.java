package com.github.lisama569.idempotenttaskqueuejava.persistence;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import java.time.Instant;

/**
 * 持久化日志事件。所有事件的应用都是幂等的（全量状态覆盖 / 单调取大），
 * 因此“快照 + 旧日志重放”在压缩中途崩溃时仍然安全。
 */
public sealed interface JournalEvent {

    /** 新任务入队（携带全部不可变字段）。 */
    record TaskEnqueued(long id, String businessKey, String type, String payload,
                        int maxAttempts, Instant enqueuedAt) implements JournalEvent {
    }

    /** 任务可变状态的全量快照（每次状态迁移后追加）。 */
    record TaskStatusChanged(long id, TaskStatus status, int attempts,
                             Instant nextVisibleAt, Instant lastFailureAt,
                             String lastErrorMessage, boolean sideEffectDone,
                             int staleDeliveries, long version) implements JournalEvent {
    }

    /** 消费位点提交。 */
    record OffsetCommitted(long committed) implements JournalEvent {
    }

    /** 累计计数器快照（接受数 / 重复拒绝数 / 容量拒绝数）。 */
    record CountersUpdated(long acceptedTotal, long rejectedDuplicateTotal,
                           long rejectedCapacityTotal) implements JournalEvent {
    }
}
