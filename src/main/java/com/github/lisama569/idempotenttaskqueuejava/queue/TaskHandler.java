package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;

/**
 * 业务处理器：实现必须自身幂等（框架保证 at-least-once 投递，并通过 EffectTracker 防重）。
 * 抛出异常视为本次消费失败，进入重试判定。
 */
public interface TaskHandler {
    void handle(TaskContext context) throws Exception;
}
