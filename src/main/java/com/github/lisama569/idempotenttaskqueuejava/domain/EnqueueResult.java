package com.github.lisama569.idempotenttaskqueuejava.domain;

/**
 * 投递结果：要么被接受（携带任务ID），要么被拒绝（携带可区分原因与既有任务ID）。
 */
public final class EnqueueResult {
    private final boolean accepted;
    private final Long taskId;
    private final RejectReason rejectReason;
    private final Long existingTaskId;
    private final String detail;

    private EnqueueResult(boolean accepted, Long taskId, RejectReason rejectReason,
                          Long existingTaskId, String detail) {
        this.accepted = accepted;
        this.taskId = taskId;
        this.rejectReason = rejectReason;
        this.existingTaskId = existingTaskId;
        this.detail = detail;
    }

    public static EnqueueResult accepted(long taskId) {
        return new EnqueueResult(true, taskId, null, null, "accepted");
    }

    public static EnqueueResult rejected(RejectReason reason, Long existingTaskId, String detail) {
        return new EnqueueResult(false, null, reason, existingTaskId, detail);
    }

    public boolean isAccepted() { return accepted; }
    public Long getTaskId() { return taskId; }
    public RejectReason getRejectReason() { return rejectReason; }
    public Long getExistingTaskId() { return existingTaskId; }
    public String getDetail() { return detail; }
}
