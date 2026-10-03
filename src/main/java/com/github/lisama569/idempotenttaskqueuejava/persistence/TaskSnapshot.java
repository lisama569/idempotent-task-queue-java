package com.github.lisama569.idempotenttaskqueuejava.persistence;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import java.time.Instant;

/**
 * 任务全量状态在快照中的表示（不可变字段 + 可变字段）。
 */
public record TaskSnapshot(
        long id, String businessKey, String type, String payload,
        int maxAttempts, Instant enqueuedAt,
        TaskStatus status, int attempts,
        Instant nextVisibleAt, Instant lastFailureAt,
        String lastErrorMessage, boolean sideEffectDone,
        int staleDeliveries, long version
) {
}
