package com.github.lisama569.idempotenttaskqueuejava.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.ArrayList;
import java.util.List;

/**
 * 单个任务的完整状态记录。该记录同时是 WAL 的快照载体：
 * 每次状态迁移都会把整条记录追加到日志，重启回放后得到最新状态。
 */
public class TaskRecord {

    /** 任务唯一 ID（系统生成）。 */
    private String id;
    /** 业务键：同一业务键全局唯一，重复投递据此判定。 */
    private String businessKey;
    /** 任务类型，用于选择处理器。 */
    private String type;
    /** 业务负载（字符串，按约定为 JSON）。 */
    private String payload;
    /** 单调递增序号，即该任务在日志中的消费位点候选值。 */
    private long seq;
    private TaskStatus status;
    /** 已经尝试执行的次数（首次执行为 1）。 */
    private int attempts;
    /** 最大尝试次数，超过后进入 FAILED 终态。 */
    private int maxAttempts;
    private long createdAt;
    private long updatedAt;
    /** 下次可被重新调度的时间（epochMillis），0 表示无等待。 */
    private long nextRetryAt;
    /** 最近一次失败原因。 */
    private String lastError;
    /** 历次失败原因，用于结果的可解释性。 */
    private List<String> failureReasons = new ArrayList<>();

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getBusinessKey() { return businessKey; }
    public void setBusinessKey(String businessKey) { this.businessKey = businessKey; }

    public String getType() { return type; }
    public void setType(String type) { this.type = type; }

    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }

    public long getSeq() { return seq; }
    public void setSeq(long seq) { this.seq = seq; }

    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }

    public int getAttempts() { return attempts; }
    public void setAttempts(int attempts) { this.attempts = attempts; }

    public int getMaxAttempts() { return maxAttempts; }
    public void setMaxAttempts(int maxAttempts) { this.maxAttempts = maxAttempts; }

    public long getCreatedAt() { return createdAt; }
    public void setCreatedAt(long createdAt) { this.createdAt = createdAt; }

    public long getUpdatedAt() { return updatedAt; }
    public void setUpdatedAt(long updatedAt) { this.updatedAt = updatedAt; }

    public long getNextRetryAt() { return nextRetryAt; }
    public void setNextRetryAt(long nextRetryAt) { this.nextRetryAt = nextRetryAt; }

    public String getLastError() { return lastError; }
    public void setLastError(String lastError) { this.lastError = lastError; }

    public List<String> getFailureReasons() { return failureReasons; }
    public void setFailureReasons(List<String> failureReasons) {
        this.failureReasons = failureReasons == null ? new ArrayList<>() : failureReasons;
    }

    /** 是否处于不可再迁移的终态。 */
    @JsonIgnore
    public boolean isTerminal() {
        return status == TaskStatus.SUCCEEDED || status == TaskStatus.FAILED;
    }
}
