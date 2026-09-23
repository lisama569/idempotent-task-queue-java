package com.github.lisama569.idempotenttaskqueuejava.domain;

/**
 * 队列可观测统计快照。
 */
public record QueueStats(
        long capacity,
        long activeCount,
        long pending,
        long delivering,
        long retryWaiting,
        long succeeded,
        long failedPermanent,
        long acceptedTotal,
        long rejectedDuplicateTotal,
        long rejectedInvalidTotal,
        long rejectedCapacityTotal,
        long committedOffset,
        boolean workerRunning
) {
}
