package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 默认本地处理器：无外部依赖，仅打印“副作用执行”日志作为可观察证据。
 * 约定：真正的副作用执行前/后调用 {@link TaskContext#markSideEffectDone()}，
 * 使得崩溃重投时第二次进入可跳过副作用（at-least-once 投递 + 幂等守卫）。
 */
public class DefaultTaskHandler implements TaskHandler {
    private static final Logger log = LoggerFactory.getLogger(DefaultTaskHandler.class);

    @Override
    public void handle(TaskContext context) {
        TaskRecord t = context.task();
        if (context.sideEffectDone()) {
            log.info("handler skip side-effect (idempotent guard): taskId={}, attempt={}, businessKey={}",
                    t.getId(), context.attempt(), t.getBusinessKey());
            return;
        }
        log.info("handler execute side-effect: taskId={}, attempt={}, businessKey={}, type={}, payload={}",
                t.getId(), context.attempt(), t.getBusinessKey(), t.getType(), t.getPayload());
        context.markSideEffectDone();
    }
}
