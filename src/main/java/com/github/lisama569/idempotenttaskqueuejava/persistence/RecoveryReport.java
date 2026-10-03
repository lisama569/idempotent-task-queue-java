package com.github.lisama569.idempotenttaskqueuejava.persistence;

import java.util.List;

/**
 * 一次恢复的完整诊断报告：恢复到什么进度、恢复到哪个位置、判断依据。
 */
public record RecoveryReport(
        boolean snapshotUsed,
        long snapshotCreatedAtEpochMillis,
        int snapshotTaskCount,
        long journalBytesReplayed,
        long journalEventsReplayed,
        long tornTailBytesTruncated,
        int deliveringResetCount,
        long offsetBeforeFix,
        long committedOffset,
        boolean offsetAdvancedOverTerminalPrefix,
        int taskCount,
        long sequence,
        long acceptedTotal,
        long rejectedDuplicateTotal,
        long rejectedCapacityTotal,
        List<String> notes
) {
    public String summarize() {
        return "RecoveryReport{snapshotUsed=" + snapshotUsed
                + ", snapshotTasks=" + snapshotTaskCount
                + ", journalEventsReplayed=" + journalEventsReplayed
                + ", journalBytesReplayed=" + journalBytesReplayed
                + ", tornTailBytesTruncated=" + tornTailBytesTruncated
                + ", deliveringReset=" + deliveringResetCount
                + ", offsetBeforeFix=" + offsetBeforeFix
                + ", committedOffset=" + committedOffset
                + ", offsetAdvancedOverTerminalPrefix=" + offsetAdvancedOverTerminalPrefix
                + ", tasks=" + taskCount + ", sequence=" + sequence
                + ", accepted=" + acceptedTotal
                + ", dupRejected=" + rejectedDuplicateTotal
                + ", capRejected=" + rejectedCapacityTotal + "}";
    }
}
