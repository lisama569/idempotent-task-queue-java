package com.github.lisama569.idempotenttaskqueuejava.store;

import com.github.lisama569.idempotenttaskqueuejava.domain.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 任务的内存存储与并发控制点：所有状态读写都在此组件的单一锁（monitor）内完成，
 * 保证“检查业务键去重 + 容量检查 + 插入”整体原子。
 *
 * <p>业务键去重覆盖全生命周期（含终态）：同一 businessKey 永久拒绝重复投递，
 * 拒绝结果中携带既有任务ID与状态，拒绝原因可解释。</p>
 */
public class TaskStore {

    private final int capacity;
    private final Object monitor = new Object();

    /** ID -> 记录；LinkedHashMap 保证按投递分配的 ID 升序，即 FIFO 顺序。 */
    private final Map<Long, TaskRecord> byId = new LinkedHashMap<>();
    /** 业务键 -> 记录（含终态记录，永久去重）。 */
    private final Map<String, TaskRecord> byBusinessKey = new LinkedHashMap<>();

    private long sequence = 0L;
    private int activeCount = 0;

    private long acceptedTotal = 0L;
    private long rejectedDuplicateTotal = 0L;
    private long rejectedCapacityTotal = 0L;

    public TaskStore(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be positive: " + capacity);
        }
        this.capacity = capacity;
    }

    /**
     * 原子入队：去重/容量判定与插入在同一把锁内，杜绝并发下的重复插入与超卖。
     */
    public EnqueueResult enqueue(String businessKey, String type, String payload,
                                 int maxAttempts, Instant now) {
        synchronized (monitor) {
            TaskRecord existing = byBusinessKey.get(businessKey);
            if (existing != null) {
                rejectedDuplicateTotal++;
                return EnqueueResult.rejected(
                        RejectReason.DUPLICATE_BUSINESS_KEY,
                        existing.getId(),
                        "businessKey already exists: taskId=" + existing.getId()
                                + ", status=" + existing.getStatus());
            }
            if (activeCount >= capacity) {
                rejectedCapacityTotal++;
                return EnqueueResult.rejected(
                        RejectReason.QUEUE_FULL,
                        null,
                        "queue full: active=" + activeCount + ", capacity=" + capacity);
            }
            long id = ++sequence;
            TaskRecord record = new TaskRecord(id, businessKey, type, payload, maxAttempts, now);
            byId.put(id, record);
            byBusinessKey.put(businessKey, record);
            activeCount++;
            acceptedTotal++;
            return EnqueueResult.accepted(id);
        }
    }

    public Optional<TaskRecord> findById(long id) {
        synchronized (monitor) {
            return Optional.ofNullable(byId.get(id));
        }
    }

    public Optional<TaskRecord> findByBusinessKey(String businessKey) {
        synchronized (monitor) {
            return Optional.ofNullable(byBusinessKey.get(businessKey));
        }
    }

    public List<TaskView> snapshotAll() {
        synchronized (monitor) {
            List<TaskView> views = new ArrayList<>(byId.size());
            for (TaskRecord r : byId.values()) {
                views.add(TaskView.from(r));
            }
            return views;
        }
    }

    /**
     * 原子领取下一个可投递任务：
     * <ul>
     *   <li>按 ID 升序（FIFO）扫描，第一个 PENDING/RETRY_WAITING 且到达可见时间的任务；</li>
     *   <li>或 DELIVERING 但超过 deliveryTimeout 未 ACK 的僵死任务（崩溃重投）；</li>
     *   <li>领取即置 DELIVERING，并把 nextVisibleAt 设为僵死判定截止线。</li>
     * </ul>
     * 队首不可见时不阻塞后续任务？——本实现严格 FIFO：队首在退避等待时，后续任务也不跳过，
     * 以保证“可观察顺序不被打乱”。
     */
    public Optional<TaskRecord> claimNext(Instant now, long deliveryTimeoutMillis) {
        synchronized (monitor) {
            for (TaskRecord r : byId.values()) {
                TaskStatus s = r.getStatus();
                if (s.isTerminal()) {
                    // 已完成（位点已推进）：跳过。终态记录保留用于业务键永久去重与状态查询。
                    continue;
                }
                if (s == TaskStatus.PENDING || s == TaskStatus.RETRY_WAITING) {
                    if (!r.getNextVisibleAt().isAfter(now)) {
                        int attempt = r.markDelivering(now.plusMillis(deliveryTimeoutMillis));
                        return Optional.of(r);
                    }
                    // 队首尚未到可见时间：严格 FIFO，不跳过，返回空。
                    return Optional.empty();
                }
                if (s == TaskStatus.DELIVERING && !r.getNextVisibleAt().isAfter(now)) {
                    // 僵死重投：同一任务再次投递，消费侧必须以幂等守卫去重副作用。
                    int attempt = r.markDelivering(now.plusMillis(deliveryTimeoutMillis));
                    r.countStaleDelivery();
                    return Optional.of(r);
                }
                if (s == TaskStatus.DELIVERING) {
                    // 队首正在投递且未超时：不跳过。
                    return Optional.empty();
                }
                // SUCCEEDED / FAILED_PERMANENT：扫描下一个（理论上终态任务位点之后才会被扫到）
            }
            return Optional.empty();
        }
    }

    /** 在存储锁内执行原子复合操作（例如 终态迁移 + 位点提交 联动）。锁可重入。 */
    public boolean completeSuccess(long taskId, Instant now) {
        synchronized (monitor) {
            TaskRecord r = byId.get(taskId);
            if (r == null || r.getStatus().isTerminal()) {
                return false;
            }
            r.markSucceeded(now);
            activeCount--;
            return true;
        }
    }

    /**
     * 失败上报：仅 DELIVERING 可迁移；重复/迟到的 NACK 不生效。
     * 按 RetryDecision 进入 RETRY_WAITING 或 FAILED_PERMANENT（重试耗尽终态，可查询）。
     */
    public Optional<TaskRecord> completeFailure(long taskId, String errorMessage,
                                                RetryDecision decision, Instant now) {
        synchronized (monitor) {
            TaskRecord r = byId.get(taskId);
            if (r == null || r.getStatus().isTerminal()) {
                return Optional.ofNullable(r);
            }
            if (decision.retry()) {
                r.markRetryWaiting(now.plus(decision.backoff()), errorMessage, now);
            } else {
                r.markFailedPermanent(errorMessage, now);
                activeCount--;
            }
            return Optional.of(r);
        }
    }

    public boolean isTerminal(long taskId) {
        synchronized (monitor) {
            TaskRecord r = byId.get(taskId);
            return r != null && r.getStatus().isTerminal();
        }
    }

    public int activeCount() {
        synchronized (monitor) {
            return activeCount;
        }
    }

    public int capacity() {
        return capacity;
    }

    public int countByStatus(TaskStatus status) {
        synchronized (monitor) {
            int n = 0;
            for (TaskRecord r : byId.values()) {
                if (r.getStatus() == status) {
                    n++;
                }
            }
            return n;
        }
    }

    public long acceptedTotal() { synchronized (monitor) { return acceptedTotal; } }
    public long rejectedDuplicateTotal() { synchronized (monitor) { return rejectedDuplicateTotal; } }
    public long rejectedCapacityTotal() { synchronized (monitor) { return rejectedCapacityTotal; } }
    public long totalTasks() { synchronized (monitor) { return byId.size(); } }

    /**
     * 恢复入口（仅供持久化层在实例新建后、对外服务前调用）：
     * 把快照+日志重放出的完整状态一次性装载进存储。
     * 要求存储为空，否则说明使用方式错误，直接拒绝。
     */
    public void restoreState(List<TaskRecord> records, long sequence,
                             long acceptedTotal, long rejectedDuplicateTotal,
                             long rejectedCapacityTotal) {
        synchronized (monitor) {
            if (!byId.isEmpty() || this.sequence != 0) {
                throw new IllegalStateException("restoreState requires a fresh, empty store");
            }
            int active = 0;
            for (TaskRecord r : records) {
                byId.put(r.getId(), r);
                byBusinessKey.put(r.getBusinessKey(), r);
                if (!r.getStatus().isTerminal()) {
                    active++;
                }
            }
            this.sequence = sequence;
            this.activeCount = active;
            this.acceptedTotal = acceptedTotal;
            this.rejectedDuplicateTotal = rejectedDuplicateTotal;
            this.rejectedCapacityTotal = rejectedCapacityTotal;
        }
    }

    /** 导出当前全部记录（供快照收拢使用）；调用方须在 {@link #withLock} 内使用返回值。 */
    public List<TaskRecord> exportRecords() {
        synchronized (monitor) {
            return new ArrayList<>(byId.values());
        }
    }

    public long currentSequence() {
        synchronized (monitor) {
            return sequence;
        }
    }

    /** 在存储锁内执行原子复合操作（例如 终态迁移 + 位点提交 联动）。锁可重入。 */
    public <T> T withLock(java.util.function.Supplier<T> action) {
        synchronized (monitor) {
            return action.get();
        }
    }

    public void withLock(Runnable action) {
        synchronized (monitor) {
            action.run();
        }
    }
}
