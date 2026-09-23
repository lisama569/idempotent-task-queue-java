package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;

/** 处理器执行上下文。 */
public interface TaskContext {
    TaskRecord task();
    int attempt();
    /** 标记副作用已完成；配合幂等守卫在重复投递时跳过副作用。 */
    void markSideEffectDone();
    boolean sideEffectDone();
}
