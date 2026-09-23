package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.queue.TaskContext;
import com.github.lisama569.idempotenttaskqueuejava.queue.TaskHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/** 测试用处理器集合。 */
public final class TestHandlers {

    private TestHandlers() {
    }

    /**
     * 可观测处理器：
     * - invocations：进入 handler 的总次数（含重复投递）；
     * - sideEffects：副作用真实执行次数（幂等守卫生效时应等于“被接受的不同任务数”）；
     * - failUntilSuccessFor：指定任务在成功前先失败若干次。
     */
    public static final class CountingHandler implements TaskHandler {
        private static final Logger log = LoggerFactory.getLogger(CountingHandler.class);

        public final AtomicInteger invocations = new AtomicInteger();
        public final AtomicInteger sideEffects = new AtomicInteger();
        public final Map<Long, AtomicInteger> perTaskInvocations = new ConcurrentHashMap<>();
        private final Map<Long, Integer> failTimesByTask = new ConcurrentHashMap<>();
        private volatile boolean failAll = false;

        /** 令指定任务在第 (failTimes) 次以内调用都抛错，之后成功。 */
        public void failTimes(long taskId, int failTimes) {
            failTimesByTask.put(taskId, failTimes);
        }

        public void failAlways(boolean fail) {
            this.failAll = fail;
        }

        @Override
        public void handle(TaskContext context) throws Exception {
            TaskRecord t = context.task();
            int n = perTaskInvocations.computeIfAbsent(t.getId(), k -> new AtomicInteger()).incrementAndGet();
            invocations.incrementAndGet();
            log.info("[TEST-HANDLER] input taskId={}, businessKey={}, attempt={}, invocation#={}, sideEffectDone={}, committedPosition=before-ack",
                    t.getId(), t.getBusinessKey(), context.attempt(), n, context.sideEffectDone());

            if (context.sideEffectDone()) {
                // 崩溃重投/重复消费：幂等守卫，绝不重复执行副作用
                log.info("[TEST-HANDLER] decision taskId={} -> SKIP side-effect (already done), basis=sideEffectDone flag",
                        t.getId());
                return;
            }

            int remainingFails = failTimesByTask.getOrDefault(t.getId(), 0);
            if (failAll || n <= remainingFails) {
                log.info("[TEST-HANDLER] decision taskId={} -> THROW (planned failure #{}), basis=fail injection",
                        t.getId(), n);
                throw new IllegalStateException("planned failure for taskId=" + t.getId() + " invocation=" + n);
            }

            sideEffects.incrementAndGet();
            context.markSideEffectDone();
            log.info("[TEST-HANDLER] decision taskId={} -> EXECUTE side-effect (count={}), basis=first effective execution",
                    t.getId(), sideEffects.get());
        }
    }
}
