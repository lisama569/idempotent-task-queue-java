package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.handler.TaskHandlerRegistry;
import com.github.lisama569.idempotenttaskqueuejava.model.OffsetCommitResult;
import com.github.lisama569.idempotenttaskqueuejava.model.QueueSnapshot;
import com.github.lisama569.idempotenttaskqueuejava.model.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.model.SubmitResult;
import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.model.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.retry.RetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.storage.WalEntry;
import com.github.lisama569.idempotenttaskqueuejava.storage.WalStore;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 幂等任务队列核心：投递去重、容量/速率约束、严格 FIFO 调度、位点推进。
 *
 * <p>判定顺序（在同一把队列锁内完成，保证并发投递结论确定）：
 * <ol>
 *   <li>输入校验：businessKey / type 非空，否则 VALIDATION_FAILED；</li>
 *   <li>业务键去重：键已存在则 DUPLICATE_BUSINESS_KEY（不创建任何任务）；</li>
 *   <li>容量校验：未完成任务数达到 capacity 则 CAPACITY_EXCEEDED；</li>
 *   <li>速率校验：滑动 1 秒窗口接受数达到上限则 RATE_LIMITED；</li>
 *   <li>分配序号、写 ENQUEUE 日志（fsync）后接受。</li>
 * </ol>
 * 所有拒绝都不消耗序号、不影响已排队任务。
 *
 * <p>位点语义：位点为“已完成（到达终态）的连续任务前缀的最大序号”。
 * 提交位点不允许回退（&lt; 当前值拒绝 ROLLBACK）；相等为幂等成功；
 * 超过当前连续可推进位点则拒绝 OUT_OF_RANGE。
 */
public class IdempotentTaskQueue {

    private final WalStore wal;
    private final TaskHandlerRegistry registry;
    private final RetryPolicy retryPolicy;
    private final int capacity;
    private final int rateLimitPerSecond;

    /** seq -> 任务（seq 从 1 开始）。 */
    private final java.util.TreeMap<Long, TaskRecord> tasks = new java.util.TreeMap<>();
    /** businessKey -> seq，投递去重与查询。 */
    private final java.util.HashMap<String, Long> keyIndex = new java.util.HashMap<>();
    /** 速率限制滑动窗口：保存最近接受投递的时间戳（毫秒）。 */
    private final Deque<Long> acceptWindow = new ArrayDeque<>();

    private long nextSeq = 1L;
    private long committedOffset = 0L;
    private long enqueuedTotal = 0L;
    private long rejectedTotal = 0L;

    public IdempotentTaskQueue(WalStore wal, TaskHandlerRegistry registry,
                               RetryPolicy retryPolicy, int capacity,
                               int rateLimitPerSecond) {
        this.wal = wal;
        this.registry = registry;
        this.retryPolicy = retryPolicy;
        this.capacity = capacity;
        this.rateLimitPerSecond = rateLimitPerSecond;
    }

    /** 回放 WAL，重建内存状态；上次崩溃时卡在 RUNNING 的任务复位为 PENDING 并补写日志。 */
    public synchronized void recover() {
        long recoveredOffset = 0L;
        for (WalEntry e : wal.replayAll()) {
            switch (e.kind()) {
                case WalEntry.KIND_ENQUEUE, WalEntry.KIND_UPDATE -> {
                    TaskRecord t = e.task();
                    // 回放中的 RUNNING 说明上次进程在任务执行中崩溃：至少一次，重新投递
                    if (t.getStatus() == TaskStatus.RUNNING) {
                        t.setStatus(TaskStatus.PENDING);
                        t.setUpdatedAt(System.currentTimeMillis());
                        t.setLastError(t.getLastError() == null
                                ? "恢复时检测到 RUNNING 状态，复位为 PENDING 重新投递"
                                : t.getLastError());
                    }
                    tasks.put(t.getSeq(), t);
                    keyIndex.put(t.getBusinessKey(), t.getSeq());
                    if (t.getSeq() >= nextSeq) {
                        nextSeq = t.getSeq() + 1;
                    }
                }
                case WalEntry.KIND_OFFSET -> recoveredOffset = e.value();
                default -> { }
            }
        }
        this.committedOffset = Math.max(recoveredOffset, computeContiguousOffset());
        // 接受总数可从日志派生，重启后重建（拒绝总数为运行期统计，重启归零）
        this.enqueuedTotal = tasks.size();
        // 将复位的 RUNNING 任务补写 UPDATE 日志，保证再次重启时状态一致
        for (TaskRecord t : tasks.values()) {
            if (t.getStatus() == TaskStatus.PENDING && t.getAttempts() > 0) {
                persistUpdate(t);
            }
        }
    }

    /** 投递任务。判定顺序见类注释。 */
    public synchronized SubmitResult submit(String businessKey, String type, String payload,
                                            Integer maxAttempts) {
        // 1. 输入校验（业务校验失败与容量/速率拒绝原因可区分）
        if (businessKey == null || businessKey.isBlank()) {
            rejectedTotal++;
            return SubmitResult.rejected(RejectReason.VALIDATION_FAILED,
                    "businessKey 不能为空");
        }
        if (type == null || type.isBlank()) {
            rejectedTotal++;
            return SubmitResult.rejected(RejectReason.VALIDATION_FAILED,
                    "type 不能为空");
        }
        if (registry.find(type).isEmpty()) {
            rejectedTotal++;
            return SubmitResult.rejected(RejectReason.VALIDATION_FAILED,
                    "未知的任务类型: " + type);
        }

        // 2. 业务键去重：只要键存在即拒绝（含终态任务，防止副作用重复）
        Long existingSeq = keyIndex.get(businessKey);
        if (existingSeq != null) {
            TaskRecord existing = tasks.get(existingSeq);
            rejectedTotal++;
            return SubmitResult.rejected(RejectReason.DUPLICATE_BUSINESS_KEY, existing,
                    "业务键已存在: " + businessKey + "，已存在任务 seq=" + existing.getSeq()
                            + " 状态=" + existing.getStatus() + "，重复投递被幂等拒绝");
        }

        // 3. 容量校验：统计所有未到达终态的任务
        int active = 0;
        for (TaskRecord t : tasks.values()) {
            if (!t.isTerminal()) {
                active++;
            }
        }
        if (active >= capacity) {
            rejectedTotal++;
            return SubmitResult.rejected(RejectReason.CAPACITY_EXCEEDED,
                    "队列容量已满: active=" + active + " capacity=" + capacity
                            + "，已接受的排队任务不受影响");
        }

        // 4. 速率校验：清理 1 秒窗口外的接受记录
        long now = System.currentTimeMillis();
        while (!acceptWindow.isEmpty() && now - acceptWindow.peekFirst() >= 1000L) {
            acceptWindow.pollFirst();
        }
        if (acceptWindow.size() >= rateLimitPerSecond) {
            rejectedTotal++;
            return SubmitResult.rejected(RejectReason.RATE_LIMITED,
                    "投递频率超限: 最近1秒接受=" + acceptWindow.size()
                            + " 上限=" + rateLimitPerSecond);
        }

        // 5. 接受：分配序号并先落盘再对外可见
        TaskRecord t = new TaskRecord();
        t.setId(UUID.randomUUID().toString());
        t.setBusinessKey(businessKey);
        t.setType(type);
        t.setPayload(payload);
        t.setSeq(nextSeq++);
        t.setStatus(TaskStatus.PENDING);
        t.setAttempts(0);
        t.setMaxAttempts(maxAttempts == null || maxAttempts <= 0 ? 1 : maxAttempts);
        t.setCreatedAt(now);
        t.setUpdatedAt(now);
        t.setNextRetryAt(0L);

        wal.append(new WalEntry(WalEntry.KIND_ENQUEUE, t, 0L, now));
        tasks.put(t.getSeq(), t);
        keyIndex.put(businessKey, t.getSeq());
        acceptWindow.addLast(now);
        enqueuedTotal++;
        return SubmitResult.accepted(t);
    }

    public synchronized Optional<TaskRecord> getByBusinessKey(String businessKey) {
        Long seq = keyIndex.get(businessKey);
        return seq == null ? Optional.empty() : Optional.of(copyOf(tasks.get(seq)));
    }

    public synchronized Optional<TaskRecord> getBySeq(long seq) {
        TaskRecord t = tasks.get(seq);
        return t == null ? Optional.empty() : Optional.of(copyOf(t));
    }

    /**
     * 提交消费位点。
     * <ul>
     *   <li>requested &lt; committedOffset：拒绝 ROLLBACK（位点只能单调前进）；</li>
     *   <li>相等：幂等成功（重复消费/重复提交无害）；</li>
     *   <li>requested &gt; 当前连续可推进位点：拒绝 OUT_OF_RANGE（不允许跳过未完成任务）。</li>
     * </ul>
     */
    public synchronized OffsetCommitResult commitOffset(long requested) {
        if (requested < committedOffset) {
            return OffsetCommitResult.rejected(committedOffset, requested, "ROLLBACK",
                    "拒绝位点回退: 当前已提交位点=" + committedOffset
                            + "，请求位点=" + requested + "，位点只允许单调前进");
        }
        if (requested == committedOffset) {
            return OffsetCommitResult.noop(committedOffset);
        }
        long advanceable = computeContiguousOffset();
        if (requested > advanceable) {
            return OffsetCommitResult.rejected(committedOffset, requested, "OUT_OF_RANGE",
                    "拒绝越界位点提交: 请求=" + requested + " 当前连续可推进位点=" + advanceable
                            + "（存在尚未到达终态的任务，不允许跳过）");
        }
        long old = committedOffset;
        committedOffset = requested;
        wal.append(new WalEntry(WalEntry.KIND_OFFSET, null, requested, System.currentTimeMillis()));
        return OffsetCommitResult.advance(old, requested);
    }

    public synchronized long committedOffset() {
        return committedOffset;
    }

    /**
     * 严格 FIFO 领取：取序号最小的可运行任务。
     * PENDING 立即可取；RETRY_WAIT 需退避到期后可取。排在队首的任务处于退避等待时，
     * 其后的任务不会被跳过（不允许越过队首消费，顺序可观察）。
     */
    synchronized Optional<TaskRecord> claimNext() {
        long now = System.currentTimeMillis();
        for (TaskRecord t : tasks.values()) {
            if (t.isTerminal()) {
                continue;
            }
            boolean runnable = (t.getStatus() == TaskStatus.PENDING
                    || t.getStatus() == TaskStatus.RETRY_WAIT)
                    && t.getNextRetryAt() <= now;
            if (!runnable) {
                // 队首第一个非终态任务尚不可运行：不越过它，保持严格顺序
                return Optional.empty();
            }
            t.setStatus(TaskStatus.RUNNING);
            t.setAttempts(t.getAttempts() + 1);
            t.setUpdatedAt(now);
            persistUpdate(t);
            return Optional.of(copyOf(t));
        }
        return Optional.empty();
    }

    /** 执行成功：置 SUCCEEDED 终态，尝试沿连续前缀推进位点并落盘。 */
    synchronized void reportSuccess(TaskRecord view) {
        TaskRecord t = tasks.get(view.getSeq());
        if (t == null) {
            return;
        }
        long now = System.currentTimeMillis();
        t.setStatus(TaskStatus.SUCCEEDED);
        t.setUpdatedAt(now);
        t.setNextRetryAt(0L);
        persistUpdate(t);
        advanceContiguous(now);
    }

    /** 执行失败：累加失败原因；可重试则 RETRY_WAIT，否则 FAILED 终态并推进位点。 */
    synchronized void reportFailure(TaskRecord view, Exception error) {
        TaskRecord t = tasks.get(view.getSeq());
        if (t == null) {
            return;
        }
        long now = System.currentTimeMillis();
        String reason = error.getClass().getSimpleName() + ": " + error.getMessage();
        t.getFailureReasons().add("第" + t.getAttempts() + "次尝试失败 - " + reason);
        t.setLastError(reason);
        t.setUpdatedAt(now);
        if (retryPolicy.canRetry(t)) {
            t.setStatus(TaskStatus.RETRY_WAIT);
            t.setNextRetryAt(now + retryPolicy.backoffMillis(t));
        } else {
            t.setStatus(TaskStatus.FAILED);
            t.setNextRetryAt(0L);
        }
        persistUpdate(t);
        if (t.getStatus() == TaskStatus.FAILED) {
            advanceContiguous(now);
        }
    }

    public synchronized QueueSnapshot snapshot() {
        List<TaskRecord> copies = new ArrayList<>();
        int active = 0;
        for (TaskRecord t : tasks.values()) {
            copies.add(copyOf(t));
            if (!t.isTerminal()) {
                active++;
            }
        }
        copies.sort(Comparator.comparingLong(TaskRecord::getSeq));
        return new QueueSnapshot(committedOffset, nextSeq, capacity, active,
                enqueuedTotal, rejectedTotal, copies);
    }

    // ---- 内部方法（调用方必须持有队列锁）----

    /** 计算当前连续终态前缀的最大序号。 */
    private long computeContiguousOffset() {
        long offset = committedOffset;
        while (true) {
            TaskRecord next = tasks.get(offset + 1);
            if (next != null && next.isTerminal()) {
                offset++;
            } else {
                return offset;
            }
        }
    }

    /** 沿连续终态前缀尽可能推进位点并写 OFFSET 日志。 */
    private void advanceContiguous(long now) {
        long target = computeContiguousOffset();
        if (target > committedOffset) {
            committedOffset = target;
            wal.append(new WalEntry(WalEntry.KIND_OFFSET, null, target, now));
        }
    }

    private void persistUpdate(TaskRecord t) {
        wal.append(new WalEntry(WalEntry.KIND_UPDATE, t, 0L, System.currentTimeMillis()));
    }

    /** 返回快照副本，避免外部在锁外修改内部状态。 */
    private static TaskRecord copyOf(TaskRecord t) {
        TaskRecord c = new TaskRecord();
        c.setId(t.getId());
        c.setBusinessKey(t.getBusinessKey());
        c.setType(t.getType());
        c.setPayload(t.getPayload());
        c.setSeq(t.getSeq());
        c.setStatus(t.getStatus());
        c.setAttempts(t.getAttempts());
        c.setMaxAttempts(t.getMaxAttempts());
        c.setCreatedAt(t.getCreatedAt());
        c.setUpdatedAt(t.getUpdatedAt());
        c.setNextRetryAt(t.getNextRetryAt());
        c.setLastError(t.getLastError());
        c.setFailureReasons(new ArrayList<>(t.getFailureReasons()));
        return c;
    }
}
