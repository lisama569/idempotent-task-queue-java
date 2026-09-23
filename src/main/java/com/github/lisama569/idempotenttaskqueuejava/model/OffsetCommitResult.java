package com.github.lisama569.idempotenttaskqueuejava.model;

/**
 * 位点提交的判定结果。
 *
 * @param committed 是否推进成功（相等提交视为幂等成功）
 * @param oldOffset 提交前已提交位点
 * @param newOffset 请求的位点
 * @param rejected  是否被拒绝
 * @param reason    拒绝原因（ROLLBACK / OUT_OF_RANGE）
 * @param message   判定依据
 */
public record OffsetCommitResult(
        boolean committed,
        long oldOffset,
        long newOffset,
        boolean rejected,
        String reason,
        String message
) {
    public static OffsetCommitResult advance(long oldOffset, long newOffset) {
        return new OffsetCommitResult(true, oldOffset, newOffset, false, null,
                "位点已推进: " + oldOffset + " -> " + newOffset);
    }

    public static OffsetCommitResult noop(long offset) {
        return new OffsetCommitResult(true, offset, offset, false, null,
                "位点未变化，幂等提交成功: " + offset);
    }

    public static OffsetCommitResult rejected(long oldOffset, long requested, String reason, String message) {
        return new OffsetCommitResult(false, oldOffset, requested, true, reason, message);
    }
}
