package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.handler.AlwaysFailTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.FlakyTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.SuccessTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.TaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.TaskHandlerRegistry;
import com.github.lisama569.idempotenttaskqueuejava.model.OffsetCommitResult;
import com.github.lisama569.idempotenttaskqueuejava.model.QueueSnapshot;
import com.github.lisama569.idempotenttaskqueuejava.model.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.model.SubmitResult;
import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.model.TaskStatus;
import com.github.lisama569.idempotenttaskqueuejava.queue.ConsumerLoop;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import com.github.lisama569.idempotenttaskqueuejava.retry.FixedBackoffRetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.storage.WalStore;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 幂等任务队列集成测试：覆盖并发投递、重复消费、重试耗尽、位点回退、
 * 容量/速率约束与崩溃恢复。所有判定均打印输入、位点与判定依据。
 */
class IdempotentTaskQueueIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotentTaskQueueIntegrationTest.class);

    @TempDir
    Path tempDir;

    // ---------- 测试辅助 ----------

    private TaskHandlerRegistry registryWith(TaskHandler... extra) {
        TaskHandlerRegistry r = new TaskHandlerRegistry();
        r.register(new SuccessTaskHandler());
        r.register(new AlwaysFailTaskHandler());
        r.register(new FlakyTaskHandler());
        for (TaskHandler h : extra) {
            r.register(h);
        }
        return r;
    }

    private IdempotentTaskQueue newQueue(Path walFile, int capacity, int rateLimit,
                                         TaskHandlerRegistry registry) {
        IdempotentTaskQueue q = new IdempotentTaskQueue(
                new WalStore(walFile), registry,
                new FixedBackoffRetryPolicy(10), capacity, rateLimit);
        q.recover();
        return q;
    }

    private static void awaitTrue(Supplier<Boolean> cond, String desc) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 10_000;
        while (System.currentTimeMillis() < deadline) {
            if (Boolean.TRUE.equals(cond.get())) {
                return;
            }
            Thread.sleep(5);
        }
        throw new AssertionError("等待超时: " + desc);
    }

    /** 记录执行顺序的处理器：副作用即记录执行的业务键。 */
    private static class RecordingHandler implements TaskHandler {
        final List<String> executed = new CopyOnWriteArrayList<>();

        @Override
        public String type() {
            return "test.record";
        }

        @Override
        public void handle(TaskRecord task) {
            executed.add(task.getBusinessKey());
        }
    }

    // ---------- 场景 1：并发投递 + 去重 + 顺序可观察 ----------

    @Test
    void concurrentSubmitDeduplicatesAndConsumesInOrder() throws Exception {
        Path wal = tempDir.resolve("q1.wal");
        RecordingHandler recorder = new RecordingHandler();
        TaskHandlerRegistry registry = registryWith(recorder);
        IdempotentTaskQueue queue = newQueue(wal, 1000, 100_000, registry);
        ConsumerLoop consumer = new ConsumerLoop(queue, registry, 5);
        consumer.start();
        try {
            int uniqueKeys = 100;
            int threads = 16;
            int totalSubmissions = uniqueKeys * 2; // 每个业务键恰好投递 2 次
            // 并发投递（每个键 2 次），期望恰好 100 接受 + 100 去重拒绝
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            CountDownLatch ready = new CountDownLatch(threads);
            CountDownLatch start = new CountDownLatch(1);
            List<Future<List<SubmitResult>>> futures = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int from = t * totalSubmissions / threads;
                final int to = (t + 1) * totalSubmissions / threads;
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    ready.await();
                    start.await();
                    List<SubmitResult> results = new ArrayList<>();
                    for (int i = from; i < to; i++) {
                        results.add(queue.submit("key-" + (i % uniqueKeys), "test.record", "{}", 3));
                    }
                    return results;
                }));
            }
            ready.await();
            start.countDown();
            List<SubmitResult> all = new ArrayList<>();
            for (Future<List<SubmitResult>> f : futures) {
                all.addAll(f.get(30, TimeUnit.SECONDS));
            }
            pool.shutdown();

            long accepted = all.stream().filter(SubmitResult::accepted).count();
            long dupRejected = all.stream()
                    .filter(r -> !r.accepted() && r.reason() == RejectReason.DUPLICATE_BUSINESS_KEY)
                    .count();
            log.info("[场景1输入] 线程={} 每线程投递={} 业务键数={} => 总投递={}",
                    threads, uniqueKeys, uniqueKeys, all.size());
            log.info("[场景1判定] 接受={} 去重拒绝={} 期望=100/100", accepted, dupRejected);
            assertEquals(uniqueKeys, accepted, "每个业务键必须恰好被接受一次");
            assertEquals(uniqueKeys, dupRejected, "其余重复投递必须被去重拒绝");

            // 序号必须连续无空洞：无任务丢失
            QueueSnapshot snap = queue.snapshot();
            List<Long> seqs = snap.tasks().stream().map(TaskRecord::getSeq).sorted().toList();
            for (int i = 0; i < seqs.size(); i++) {
                assertEquals(i + 1L, seqs.get(i), "序号必须连续，不允许丢任务");
            }

            // 等待全部消费完成
            awaitTrue(() -> queue.committedOffset() == uniqueKeys, "全部任务消费完成");
            log.info("[场景1位点] 最终提交位点={} 执行次数={}",
                    queue.committedOffset(), consumer.executionCount());
            assertEquals(uniqueKeys, consumer.executionCount(), "每个任务恰好执行一次，无副作用重复");

            // 顺序可观察：执行顺序必须与 seq 顺序一致（严格 FIFO）
            Map<String, Long> seqByKey = new ConcurrentHashMap<>();
            for (TaskRecord t : queue.snapshot().tasks()) {
                seqByKey.put(t.getBusinessKey(), t.getSeq());
            }
            List<Long> executedSeqs = recorder.executed.stream().map(seqByKey::get).toList();
            List<Long> sorted = new ArrayList<>(executedSeqs);
            sorted.sort(Long::compareTo);
            log.info("[场景1判定] 执行顺序与seq一致={} 前5个执行seq={}",
                    executedSeqs.equals(sorted), executedSeqs.subList(0, 5));
            assertEquals(sorted, executedSeqs, "执行顺序必须与入队序号一致（严格 FIFO）");
        } finally {
            consumer.stop();
        }
    }

    // ---------- 场景 2：重复消费与消费进程重启，状态不重复不丢失 ----------

    @Test
    void consumerRestartDoesNotDuplicateOrLose() throws Exception {
        Path wal = tempDir.resolve("q2.wal");
        RecordingHandler recorder = new RecordingHandler();
        TaskHandlerRegistry registry = registryWith(recorder);
        IdempotentTaskQueue queue = newQueue(wal, 100, 100_000, registry);
        ConsumerLoop consumer = new ConsumerLoop(queue, registry, 5);
        consumer.start();

        for (int i = 0; i < 5; i++) {
            assertTrue(queue.submit("r-" + i, "test.record", "{}", 3).accepted());
        }
        awaitTrue(() -> queue.committedOffset() == 5, "首批 5 个任务完成");
        long offsetBeforeRestart = queue.committedOffset();
        long executionsBeforeRestart = consumer.executionCount();
        log.info("[场景2位点] 重启前位点={} 执行次数={}", offsetBeforeRestart, executionsBeforeRestart);

        // 模拟消费进程重启：优雅停止 -> 从 WAL 重建队列 -> 重新启动
        consumer.stop();
        IdempotentTaskQueue queue2 = newQueue(wal, 100, 100_000, registry);
        ConsumerLoop consumer2 = new ConsumerLoop(queue2, registry, 5);
        consumer2.start();
        try {
            log.info("[场景2位点] 重启后恢复位点={}", queue2.committedOffset());
            assertEquals(offsetBeforeRestart, queue2.committedOffset(), "重启后位点必须恢复");

            // 重启后补投新任务，验证从断点继续且旧任务不重复执行
            assertTrue(queue2.submit("r-5", "test.record", "{}", 3).accepted());
            awaitTrue(() -> queue2.committedOffset() == 6, "重启后新任务完成");
            Thread.sleep(100); // 给潜在重复执行留出暴露时间
            log.info("[场景2判定] 执行记录={} 重启前执行次数={}",
                    recorder.executed, executionsBeforeRestart);
            assertEquals(6, recorder.executed.size(), "重启后不得重复执行已完成任务");
            assertEquals(executionsBeforeRestart, consumer.executionCount());
            // 重复投递已完成的业务键仍被拒绝
            SubmitResult dup = queue2.submit("r-0", "test.record", "{}", 3);
            log.info("[场景2判定] 重复投递已完成键 accepted={} reason={}", dup.accepted(), dup.reason());
            assertFalse(dup.accepted());
            assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dup.reason());
        } finally {
            consumer2.stop();
        }
    }

    // ---------- 场景 3：执行中崩溃恢复（RUNNING 复位，至少一次） ----------

    @Test
    void crashDuringExecutionRecoversRunningTask() throws Exception {
        Path wal = tempDir.resolve("q3.wal");
        // 首次执行永久阻塞（模拟执行中进程被 kill）；恢复后由新队列重新投递时放行
        AtomicBoolean recoveredMode = new AtomicBoolean(false);
        List<String> executed = new CopyOnWriteArrayList<>();
        TaskHandler blocking = new TaskHandler() {
            @Override
            public String type() {
                return "test.blocking";
            }

            @Override
            public void handle(TaskRecord task) throws Exception {
                executed.add(task.getBusinessKey() + "#attempt" + task.getAttempts());
                if (!recoveredMode.get()) {
                    // 被放弃的 daemon 线程在此等到 JVM 退出，不会写任何终态日志
                    while (true) {
                        Thread.sleep(60_000);
                    }
                }
            }
        };
        TaskHandlerRegistry registry = registryWith(blocking);
        IdempotentTaskQueue queue = newQueue(wal, 100, 100_000, registry);
        ConsumerLoop consumer = new ConsumerLoop(queue, registry, 5);
        consumer.start();

        assertTrue(queue.submit("crash-1", "test.blocking", "{}", 3).accepted());
        awaitTrue(() -> !executed.isEmpty(), "任务进入执行中");
        // 模拟崩溃：不等待任务完成直接放弃；WAL 中任务停留在 RUNNING
        consumer.abandonForCrashSimulation();

        IdempotentTaskQueue queue2 = newQueue(wal, 100, 100_000, registry);
        TaskRecord recovered = queue2.getByBusinessKey("crash-1").orElseThrow();
        log.info("[场景3判定] 恢复后状态={} attempts={} 位点={}",
                recovered.getStatus(), recovered.getAttempts(), queue2.committedOffset());
        assertEquals(TaskStatus.PENDING, recovered.getStatus(), "RUNNING 任务必须复位为 PENDING");

        recoveredMode.set(true);
        ConsumerLoop consumer2 = new ConsumerLoop(queue2, registry, 5);
        consumer2.start();
        try {
            awaitTrue(() -> queue2.committedOffset() == 1, "恢复后任务重新投递并完成");
            log.info("[场景3位点] 最终位点={} 执行记录={}", queue2.committedOffset(), executed);
            assertEquals(2, executed.size(), "至少一次：崩溃前执行1次 + 恢复后重投1次");
            assertTrue(executed.get(0).endsWith("#attempt1"));
            assertTrue(executed.get(1).endsWith("#attempt2"));
        } finally {
            consumer2.stop();
        }
    }

    // ---------- 场景 4：位点回退被拒绝，越界被拒绝，相等幂等 ----------

    @Test
    void offsetRollbackRejectedAndEqualCommitIdempotent() throws Exception {
        Path wal = tempDir.resolve("q4.wal");
        TaskHandlerRegistry registry = registryWith();
        IdempotentTaskQueue queue = newQueue(wal, 100, 100_000, registry);
        ConsumerLoop consumer = new ConsumerLoop(queue, registry, 5);
        consumer.start();
        try {
            for (int i = 0; i < 3; i++) {
                assertTrue(queue.submit("o-" + i, "demo.success", "{}", 3).accepted());
            }
            awaitTrue(() -> queue.committedOffset() == 3, "3 个任务完成");
            log.info("[场景4输入] 当前位点=3，依次提交 2 / 3 / 5");

            OffsetCommitResult rollback = queue.commitOffset(2);
            log.info("[场景4判定] 回退提交 rejected={} reason={} 依据={}",
                    rollback.rejected(), rollback.reason(), rollback.message());
            assertTrue(rollback.rejected());
            assertEquals("ROLLBACK", rollback.reason());
            assertEquals(3, queue.committedOffset(), "位点不得回退");

            OffsetCommitResult equal = queue.commitOffset(3);
            log.info("[场景4判定] 相等提交 committed={} 依据={}", equal.committed(), equal.message());
            assertTrue(equal.committed(), "相等提交必须幂等成功");
            assertEquals(3, queue.committedOffset());

            OffsetCommitResult outOfRange = queue.commitOffset(5);
            log.info("[场景4判定] 越界提交 rejected={} reason={} 依据={}",
                    outOfRange.rejected(), outOfRange.reason(), outOfRange.message());
            assertTrue(outOfRange.rejected());
            assertEquals("OUT_OF_RANGE", outOfRange.reason());
            assertEquals(3, queue.committedOffset());
        } finally {
            consumer.stop();
        }
    }

    // ---------- 场景 5：重试耗尽进入 FAILED 终态，可查询且原因可解释 ----------

    @Test
    void retryExhaustionLeadsToFailedTerminalState() throws Exception {
        Path wal = tempDir.resolve("q5.wal");
        TaskHandlerRegistry registry = registryWith();
        IdempotentTaskQueue queue = newQueue(wal, 100, 100_000, registry);
        ConsumerLoop consumer = new ConsumerLoop(queue, registry, 5);
        consumer.start();
        try {
            assertTrue(queue.submit("fail-1", "demo.always-fail", "{}", 3).accepted());
            assertTrue(queue.submit("ok-1", "demo.success", "{}", 3).accepted());

            awaitTrue(() -> {
                TaskRecord t = queue.getByBusinessKey("fail-1").orElseThrow();
                return t.getStatus() == TaskStatus.FAILED;
            }, "fail-1 重试耗尽进入 FAILED");

            TaskRecord failed = queue.getByBusinessKey("fail-1").orElseThrow();
            log.info("[场景5判定] 终态={} attempts={} 失败原因数={} 位点={}",
                    failed.getStatus(), failed.getAttempts(),
                    failed.getFailureReasons().size(), queue.committedOffset());
            failed.getFailureReasons().forEach(r -> log.info("[场景5依据] {}", r));
            assertEquals(3, failed.getAttempts(), "必须恰好尝试 maxAttempts 次");
            assertEquals(3, failed.getFailureReasons().size(), "每次失败原因可查询");
            assertTrue(failed.isTerminal());

            // 失败任务到达终态后位点照常推进，后续任务不受影响
            awaitTrue(() -> queue.committedOffset() == 2, "后续任务完成且位点推进");
            assertEquals(TaskStatus.SUCCEEDED,
                    queue.getByBusinessKey("ok-1").orElseThrow().getStatus());
            log.info("[场景5位点] 最终位点={}（含 FAILED 任务的连续前缀）", queue.committedOffset());
        } finally {
            consumer.stop();
        }
    }

    // ---------- 场景 6：失败后重试成功 ----------

    @Test
    void flakyTaskSucceedsAfterRetries() throws Exception {
        Path wal = tempDir.resolve("q6.wal");
        TaskHandlerRegistry registry = registryWith();
        IdempotentTaskQueue queue = newQueue(wal, 100, 100_000, registry);
        ConsumerLoop consumer = new ConsumerLoop(queue, registry, 5);
        consumer.start();
        try {
            assertTrue(queue.submit("flaky-1", "demo.flaky", "{}", 3).accepted());
            awaitTrue(() -> queue.committedOffset() == 1, "flaky 任务最终成功");
            TaskRecord t = queue.getByBusinessKey("flaky-1").orElseThrow();
            log.info("[场景6判定] 状态={} attempts={} 历史失败={}",
                    t.getStatus(), t.getAttempts(), t.getFailureReasons());
            assertEquals(TaskStatus.SUCCEEDED, t.getStatus());
            assertEquals(3, t.getAttempts(), "前两次失败后第三次成功");
            assertEquals(2, t.getFailureReasons().size());
        } finally {
            consumer.stop();
        }
    }

    // ---------- 场景 7：容量 / 速率 / 校验失败原因可区分，且不影响已排队任务 ----------

    @Test
    void capacityRateAndValidationRejectionsAreDistinguishable() throws Exception {
        Path wal = tempDir.resolve("q7.wal");
        TaskHandlerRegistry registry = registryWith();
        // 容量 2，速率上限 3/秒
        IdempotentTaskQueue queue = newQueue(wal, 2, 3, registry);

        SubmitResult a = queue.submit("c-1", "demo.success", "{}", 3);
        SubmitResult b = queue.submit("c-2", "demo.success", "{}", 3);
        SubmitResult overflow = queue.submit("c-3", "demo.success", "{}", 3);
        log.info("[场景7判定] 容量: a={} b={} overflow.reason={}",
                a.accepted(), b.accepted(), overflow.reason());
        assertTrue(a.accepted() && b.accepted());
        assertFalse(overflow.accepted());
        assertEquals(RejectReason.CAPACITY_EXCEEDED, overflow.reason());

        // 速率：容量 100，上限 3/秒，第 4 个被拒
        IdempotentTaskQueue rateQueue = newQueue(tempDir.resolve("q7b.wal"), 100, 3, registry);
        List<SubmitResult> rs = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            rs.add(rateQueue.submit("rate-" + i, "demo.success", "{}", 3));
        }
        long rateRejected = rs.stream()
                .filter(r -> !r.accepted() && r.reason() == RejectReason.RATE_LIMITED).count();
        log.info("[场景7判定] 速率: 接受={} 限流拒绝={}",
                rs.stream().filter(SubmitResult::accepted).count(), rateRejected);
        assertEquals(3, rs.stream().filter(SubmitResult::accepted).count());
        assertEquals(2, rateRejected);

        // 校验失败与上述原因可区分
        SubmitResult invalid = queue.submit("  ", "demo.success", "{}", 3);
        SubmitResult unknownType = queue.submit("c-x", "no.such.type", "{}", 3);
        log.info("[场景7判定] 校验: blankKey={} unknownType={}",
                invalid.reason(), unknownType.reason());
        assertEquals(RejectReason.VALIDATION_FAILED, invalid.reason());
        assertEquals(RejectReason.VALIDATION_FAILED, unknownType.reason());

        // 已排队任务不受影响：启动消费者后全部完成
        ConsumerLoop consumer = new ConsumerLoop(queue, registry, 5);
        consumer.start();
        try {
            awaitTrue(() -> queue.committedOffset() == 2, "已接受任务正常消费");
            log.info("[场景7位点] 容量拒绝后已排队任务位点={}", queue.committedOffset());
            assertEquals(TaskStatus.SUCCEEDED,
                    queue.getByBusinessKey("c-1").orElseThrow().getStatus());
        } finally {
            consumer.stop();
        }
    }

    // ---------- 场景 8：WAL 重启恢复，状态与位点不丢不重 ----------

    @Test
    void walRecoveryPreservesStateAndOffset() throws Exception {
        Path wal = tempDir.resolve("q8.wal");
        TaskHandlerRegistry registry = registryWith();
        IdempotentTaskQueue queue = newQueue(wal, 100, 100_000, registry);
        ConsumerLoop consumer = new ConsumerLoop(queue, registry, 5);
        consumer.start();

        assertTrue(queue.submit("w-1", "demo.success", "{}", 3).accepted());
        assertTrue(queue.submit("w-2", "demo.always-fail", "{}", 2).accepted());
        awaitTrue(() -> queue.committedOffset() == 2, "两个任务到达终态");
        consumer.stop();

        // 模拟进程重启：同一 WAL 文件重建
        IdempotentTaskQueue queue2 = newQueue(wal, 100, 100_000, registry);
        log.info("[场景8位点] 恢复位点={} 任务数={}",
                queue2.committedOffset(), queue2.snapshot().tasks().size());
        assertEquals(2, queue2.committedOffset(), "位点必须从 WAL 恢复");
        assertEquals(TaskStatus.SUCCEEDED,
                queue2.getByBusinessKey("w-1").orElseThrow().getStatus());
        assertEquals(TaskStatus.FAILED,
                queue2.getByBusinessKey("w-2").orElseThrow().getStatus());

        // 重启后重复投递同业务键仍被拒绝（去重状态恢复）
        SubmitResult dup = queue2.submit("w-1", "demo.success", "{}", 3);
        log.info("[场景8判定] 重启后重复投递 reason={}", dup.reason());
        assertFalse(dup.accepted());
        assertEquals(RejectReason.DUPLICATE_BUSINESS_KEY, dup.reason());

        // 重启后位点回退仍被拒绝
        OffsetCommitResult rollback = queue2.commitOffset(1);
        assertTrue(rollback.rejected());
        assertEquals("ROLLBACK", rollback.reason());
    }
}
