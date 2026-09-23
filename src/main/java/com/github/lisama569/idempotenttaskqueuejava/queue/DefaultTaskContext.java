package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;

/**
 * 默认执行上下文：包装当前被领取的任务快照与本次尝试序号。
 * sideEffectDone 直接读写 TaskRecord 中的幂等标记（该字段在崩溃重投场景仍保留）。
 */
public class DefaultTaskContext implements TaskContext {
    private final TaskRecord task;
    private final int attempt;

    public DefaultTaskContext(TaskRecord task, int attempt) {
        this.task = task;
        this.attempt = attempt;
    }

    @Override
    public TaskRecord task() {
        return task;
    }

    @Override
    public int attempt() {
        return attempt;
    }

    @Override
    public void markSideEffectDone() {
        task.markSideEffectDone();
    }

    @Override
    public boolean sideEffectDone() {
        return task.isSideEffectDone();
    }
}
