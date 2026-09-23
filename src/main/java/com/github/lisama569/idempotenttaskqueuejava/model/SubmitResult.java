package com.github.lisama569.idempotenttaskqueuejava.model;

/**
 * 投递尝试的判定结果。
 *
 * @param accepted   是否被接受
 * @param reason     被拒绝时的可区分原因；接受时为 null
 * @param taskId     被接受时的新任务 ID；重复拒绝时为已存在任务的 ID
 * @param seq        被接受任务的序号；重复拒绝时为已存在任务的序号
 * @param existingStatus 重复拒绝时已存在任务的当前状态；否则 null
 * @param message    人类可解释的判定依据
 */
public record SubmitResult(
        boolean accepted,
        RejectReason reason,
        String taskId,
        Long seq,
        TaskStatus existingStatus,
        String message
) {
    public static SubmitResult accepted(TaskRecord t) {
        return new SubmitResult(true, null, t.getId(), t.getSeq(), null,
                "任务已接受，序号=" + t.getSeq());
    }

    public static SubmitResult rejected(RejectReason reason, TaskRecord existing, String message) {
        return new SubmitResult(false, reason,
                existing == null ? null : existing.getId(),
                existing == null ? null : existing.getSeq(),
                existing == null ? null : existing.getStatus(),
                message);
    }

    public static SubmitResult rejected(RejectReason reason, String message) {
        return rejected(reason, null, message);
    }
}
