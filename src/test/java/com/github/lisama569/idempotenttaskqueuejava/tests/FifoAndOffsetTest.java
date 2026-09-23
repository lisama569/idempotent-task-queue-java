package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.queue.DefaultTaskContext;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * 场景二：多任务按 FIFO 顺序消费，位点随终态单调推进；重复 ACK 不重复推进。
 */
class FifoAndOffsetTest {
    private static final Logger log = LoggerFactory.getLogger(FifoAndOffsetTest.class);

    @Test
    void tasksConsumeInFifoOrder_andOffsetAdvances() throws Exception {
        QueueProperties props = TestQueueFactory.props(100, 1000, 3, 20, 100);
        IdempotentTaskQueue queue = TestQueueFactory.fixedBackoffQueue(props);
        TestHandlers.CountingHandler handler = new TestHandlers.CountingHandler();

        long id1 = queue.enqueue("k1", "t", "p1").getTaskId();
        long id2 = queue.enqueue("k2", "t", "p2").getTaskId();
        long id3 = queue.enqueue("k3", "t", "p3").getTaskId();
        log.info("[TEST-INPUT] enqueued ids={},{},{} (FIFO order)", id1, id2, id3);
        assertEquals(0, queue.committedOffset(), "offset starts at 0");

        List<Long> consumed = new ArrayList<>();
        TaskRecord firstTask = null;
        for (int round = 0; round < 3; round++) {
            Optional<TaskRecord> claimed = queue.claimNext(Instant.now());
            assertTrue(claimed.isPresent());
            TaskRecord task = claimed.get();
            if (round == 0) {
                firstTask = task;
            }
            consumed.add(task.getId());
            log.info("[TEST-OFFSET] before ack: taskId={}, committedOffset={}", task.getId(), queue.committedOffset());
            handler.handle(new DefaultTaskContext(task, task.getAttempts()));
            queue.ackSuccess(task, Instant.now());
            assertEquals(task.getId(), queue.committedOffset(), "offset advances to finished task id");
        }

        log.info("[TEST-RESULT] consumed order={}, sideEffects={}", consumed, handler.sideEffects.get());
        log.info("[TEST-BASIS] 按 ID 升序 claim + 单分区 => 可观察顺序 == 投递顺序");
        assertEquals(List.of(id1, id2, id3), consumed);
        assertEquals(3, handler.sideEffects.get());
        assertTrue(queue.claimNext(Instant.now()).isEmpty());

        queue.listTasks().forEach(v -> {
            assertTrue(v.status().isTerminal(), "task " + v.id() + " terminal");
            assertEquals(TaskStatus.SUCCEEDED, v.status());
        });

        // 重复 ACK（模拟重复消费通知）幂等：任务已终态，位点不变化、不报错、副作用不重复
        handler.handle(new DefaultTaskContext(firstTask, firstTask.getAttempts()));
        queue.ackSuccess(firstTask, Instant.now());
        log.info("[TEST-OFFSET] after duplicate ack: committedOffset={}, sideEffects={}, invocations={}",
                queue.committedOffset(), handler.sideEffects.get(), handler.invocations.get());
        assertEquals(id3, queue.committedOffset(), "duplicate ack must not move offset");
        assertEquals(3, handler.sideEffects.get(), "duplicate delivery must not repeat side-effect");
    }
}
