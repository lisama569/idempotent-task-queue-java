package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskView;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景三：失败按固定退避重试；达到最大尝试次数后进入 FAILED_PERMANENT 可查询终态；
 * 随后任务位点顺序推进，不丢任务。
 */
class RetryExhaustionTest {
    private static final Logger log = LoggerFactory.getLogger(RetryExhaustionTest.class);

    @Test
    void retriesThenSucceedsWithinMaxAttempts() throws Exception {
        QueueProperties props = TestQueueFactory.props(100, 1000, 3, 30, 60_000);
        IdempotentTaskQueue queue = TestQueueFactory.fixedBackoffQueue(props);
        TestHandlers.CountingHandler handler = new TestHandlers.CountingHandler();

        long id = queue.enqueue("retry-ok", "t", null).getTaskId();
        handler.failTimes(id, 2); // 前两次失败，第三次成功
        log.info("[TEST-INPUT] taskId={}, maxAttempts={}, plannedFailures=2, backoff=30ms",
                id, props.getMaxAttempts());

        TaskStatus[] trail = new TaskStatus[3];
        for (int i = 0; i < 3; i++) {
            TaskRecord t = queue.claimNext(Instant.now()).orElseThrow();
            assertEquals(id, t.getId());
            deliver(queue, handler, t);
            trail[i] = queue.getTask(id).status();
            log.info("[TEST-RESULT] attempt={}, status={}, nextVisibleAt={}, committedOffset={}",
                    t.getAttempts(), trail[i], queue.getTask(id).nextVisibleAt(), queue.committedOffset());
            if (i < 2) {
                assertEquals(TaskStatus.RETRY_WAITING, trail[i], "failure schedules retry");
                // 退避未到不可领取
                assertTrue(queue.claimNext(Instant.now()).isEmpty(), "backoff window blocks re-claim");
                Thread.sleep(40);
            }
        }
        assertEquals(TaskStatus.SUCCEEDED, queue.getTask(id).status());
        assertEquals(1, queue.committedOffset(), "success advances offset");
        log.info("[TEST-BASIS] attempts<maxAttempts => RETRY_WAITING；退避到期后 FIFO 重新可领");
    }

    @Test
    void exhaustedAfterMaxAttempts_entersQueryableTerminalState_andOffsetAdvances() throws Exception {
        QueueProperties props = TestQueueFactory.props(100, 1000, 3, 20, 60_000);
        IdempotentTaskQueue queue = TestQueueFactory.fixedBackoffQueue(props);
        TestHandlers.CountingHandler handler = new TestHandlers.CountingHandler();
        handler.failAlways(true);

        long id1 = queue.enqueue("dead-1", "t", null).getTaskId();
        long id2 = queue.enqueue("after-dead", "t", null).getTaskId();
        log.info("[TEST-INPUT] tasks={},{}, maxAttempts={}, handler=always fail", id1, id2, props.getMaxAttempts());

        for (int attempt = 1; attempt <= 3; attempt++) {
            TaskRecord t = queue.claimNext(Instant.now()).orElseThrow();
            deliver(queue, handler, t);
            TaskView v = queue.getTask(id1);
            log.info("[TEST-RESULT] attempt={}, status={}, attempts={}, lastError={}, committedOffset={}",
                    attempt, v.status(), v.attempts(), v.lastErrorMessage(), queue.committedOffset());
            if (attempt < 3) {
                assertEquals(TaskStatus.RETRY_WAITING, v.status());
                Thread.sleep(30);
            }
        }

        TaskView dead = queue.getTask(id1);
        assertEquals(TaskStatus.FAILED_PERMANENT, dead.status(), "retry exhaustion yields terminal failure");
        assertEquals(3, dead.attempts(), "attempts capped at maxAttempts");
        assertEquals(1, queue.committedOffset(), "permanent failure still advances offset in order");
        assertNotNull(dead.lastErrorMessage());
        log.info("[TEST-BASIS] attempts==maxAttempts => FAILED_PERMANENT（可查询终态），位点推进到该任务");

        // 后续任务不丢失：被头任务阻塞后，现在可以领取并消费
        TaskRecord next = queue.claimNext(Instant.now()).orElseThrow();
        assertEquals(id2, next.getId());
        handler.failAlways(false);
        deliver(queue, handler, next);
        assertEquals(2, queue.committedOffset());
        log.info("[TEST-RESULT] following task id2={} consumed after head terminal, committedOffset={}",
                id2, queue.committedOffset());
    }

    /** 模拟 worker 的一次投递：执行 handler，按结果 ACK/NACK。 */
    private static void deliver(IdempotentTaskQueue queue, TestHandlers.CountingHandler handler, TaskRecord t) {
        try {
            handler.handle(new com.github.lisama569.idempotenttaskqueuejava.queue.DefaultTaskContext(t, t.getAttempts()));
            queue.ackSuccess(t, Instant.now());
        } catch (Exception e) {
            queue.ackFailure(t, String.valueOf(e.getMessage()), Instant.now());
        }
    }
}
