package com.github.lisama569.idempotenttaskqueuejava.persistence;

/**
 * 一次恢复过程的可观测报告：恢复到什么进度、恢复到哪个位置、判断依据。
 */
public record RecoveryReport(
        boolean snapshotLoaded,
        long snapshotCommittedOffset,
        int snapshotTaskCount,
        long journalRecordsReplayed,
        long journalTruncatedAtBytes,
        long journalTornTailBytes,
        int tasksRecovered,
        long committedOffset,
        long sequence,
        String basis
) {
    public static RecoveryReport empty(String basis) {
        return new RecoveryReport(false, 0L, 0, 0L, -1L, 0L, 0, 0L, 0L, basis);
    }
}
