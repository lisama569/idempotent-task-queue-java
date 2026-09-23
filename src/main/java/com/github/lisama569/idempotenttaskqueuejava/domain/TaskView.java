package com.github.lisama569.idempotenttaskqueuejava.domain;

import java.time.Instant;

/**
 * 任务状态查询视图（只读快照）。
 */
public record TaskView(
        long id,
        String businessKey,
        String type,
        String payload,
        TaskStatus status,
        int attempts,
        int maxAttempts,
        Instant enqueuedAt,
        Instant nextVisibleAt,
        Instant lastFailureAt,
        String lastErrorMessage,
        boolean sideEffectDone,
        int staleDeliveries,
        long version
) {
    public static TaskView from(TaskRecord r) {
        return new TaskView(
                r.getId(), r.getBusinessKey(), r.getType(), r.getPayload(),
                r.getStatus(), r.getAttempts(), r.getMaxAttempts(),
                r.getEnqueuedAt(), r.getNextVisibleAt(), r.getLastFailureAt(),
                r.getLastErrorMessage(), r.isSideEffectDone(),
                r.getStaleDeliveries(), r.getVersion()
        );
    }
}
