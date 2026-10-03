package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.domain.OffsetRejectReason;
import com.github.lisama569.idempotenttaskqueuejava.exception.OffsetRejectedException;

/**
 * 消费位点管理：
 * - 位点单调递增，回退（requested &lt; committed）一律拒绝；
 * - 不允许跳过缺口（requested &gt; committed+1 且相邻任务未终态）拒绝 GAP。
 * 重启后从已提交位点的下一个任务继续，不会重复也不会丢失。
 */
public class OffsetManager {
    private long committed;
    /** 位点提交成功后的回调（持久化层挂载点）；在调用方持有的存储锁内触发。 */
    private java.util.function.LongConsumer commitListener = c -> { };

    public OffsetManager() {
        this.committed = 0L;
    }

    /** 从持久化位点恢复（重启场景）：下一个待处理任务仍是 committed+1。 */
    public OffsetManager(long initialCommitted) {
        if (initialCommitted < 0) {
            throw new IllegalArgumentException("initialCommitted must be >= 0: " + initialCommitted);
        }
        this.committed = initialCommitted;
    }

    public void setCommitListener(java.util.function.LongConsumer commitListener) {
        this.commitListener = commitListener == null ? c -> { } : commitListener;
    }

    /** 当前已提交位点：下一个待处理任务ID为 committed+1。 */
    public long committed() {
        return committed;
    }

    /**
     * 尝试提交位点，规则（可解释、可区分）：
     * <ol>
     *   <li>{@code requested < committed}：位点回退，拒绝 {@link OffsetRejectReason#REGRESSION}；</li>
     *   <li>{@code requested == committed}：重复提交，幂等空操作，允许；</li>
     *   <li>{@code requested > committed+1}：跳过中间任务形成缺口，拒绝 {@link OffsetRejectReason#GAP}；</li>
     *   <li>{@code requested == committed+1}：仅当相邻任务已到终态才允许推进，否则拒绝 GAP。</li>
     * </ol>
     * 位点只前进、不回退，重启后从 committed+1 继续，不重复、不丢失。
     */
    public void commit(long requested, java.util.function.LongPredicate isNextTerminal) {
        if (requested < committed) {
            throw new OffsetRejectedException(requested, committed, OffsetRejectReason.REGRESSION);
        }
        if (requested == committed) {
            return; // 重复提交，幂等接受
        }
        if (requested > committed + 1) {
            throw new OffsetRejectedException(requested, committed, OffsetRejectReason.GAP);
        }
        if (!isNextTerminal.test(requested)) {
            throw new OffsetRejectedException(requested, committed, OffsetRejectReason.GAP);
        }
        committed = requested;
        commitListener.accept(requested);
    }
}
