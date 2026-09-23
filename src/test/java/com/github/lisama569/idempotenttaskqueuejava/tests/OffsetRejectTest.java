package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.domain.OffsetRejectReason;
import com.github.lisama569.idempotenttaskqueuejava.exception.OffsetRejectedException;
import com.github.lisama569.idempotenttaskqueuejava.queue.OffsetManager;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景四：消费位点兼容性范围。
 * - 只接受 committed（重复提交，幂等）与 committed+1（且任务终态）；
 * - 回退一律拒绝 REGRESSION；跳过缺口拒绝 GAP；
 * - 任务未终态时不允许越过它提交。
 */
class OffsetRejectTest {
    private static final Logger log = LoggerFactory.getLogger(OffsetRejectTest.class);

    private OffsetRejectReason runAndCapture(long committedStart, long requested,
                                             java.util.function.LongPredicate terminal) {
        OffsetManager om = new OffsetManager();
        if (committedStart > 0) {
            // 把位点推进到 committedStart：模拟 1..committedStart 都已终态
            for (long i = 1; i <= committedStart; i++) {
                long cur = i;
                om.commit(cur, id -> true);
            }
        }
        log.info("[TEST-INPUT] committed={}, requested={}", committedStart, requested);
        try {
            om.commit(requested, terminal);
            log.info("[TEST-RESULT] decision=ACCEPT, committedAfter={}", om.committed());
            return null;
        } catch (OffsetRejectedException e) {
            log.info("[TEST-RESULT] decision=REJECT, reason={}, current={}, requested={}, basis=monotonic+contiguous rule",
                    e.getReason(), e.getCurrent(), e.getRequested());
            return e.getReason();
        }
    }

    @Test
    void regressionIsRejected() {
        OffsetRejectReason reason = runAndCapture(5, 3, id -> true);
        assertEquals(OffsetRejectReason.REGRESSION, reason, "backward offset must be refused, never resumed mid-way");
    }

    @Test
    void gapIsRejected() {
        OffsetRejectReason reason = runAndCapture(5, 8, id -> true);
        assertEquals(OffsetRejectReason.GAP, reason, "skipping tasks creates a gap and must be refused");
    }

    @Test
    void commitOverNonTerminalNextIsRejected() {
        OffsetRejectReason reason = runAndCapture(5, 6, id -> false);
        assertEquals(OffsetRejectReason.GAP, reason, "cannot commit past a non-terminal adjacent task");
    }

    @Test
    void duplicateCommitIsIdempotentAndSequentialAdvanceWorks() {
        OffsetRejectReason dup = runAndCapture(5, 5, id -> true);
        assertNull(dup, "re-commit of same offset is an idempotent no-op");
        OffsetRejectReason next = runAndCapture(5, 6, id -> true);
        assertNull(next, "committed+1 over terminal task is accepted");
    }
}
