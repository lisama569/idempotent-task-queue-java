package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;

/**
 * 默认执行上下文：包装当前被领取的任务快照与本次尝试序号。
 * sideEffectDone 直接读写 TaskRecord 中的幂等标记（该字段在崩溃重投场景仍保留）。
 */
public class DefaultTaskContext implements TaskContext {
    private final TaskRecord task;
    private final int attempt;
    /** 幂等标记落点：默认仅写内存记录；ConsumerWorker 注入经队列/存储的持久化路径。 */
    private final java.util.function.Consumer<TaskRecord> sideEffectMarker;

    public DefaultTaskContext(TaskRecord task, int attempt) {
        this(task, attempt, TaskRecord::markSideEffectDone);
    }

    public DefaultTaskContext(TaskRecord task, int attempt,
                              java.util.function.Consumer<TaskRecord> sideEffectMarker) {
        this.task = task;
        this.attempt = attempt;
        this.sideEffectMarker = sideEffectMarker;
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
        sideEffectMarker.accept(task);
    }

    @Override
    public boolean sideEffectDone() {
        return task.isSideEffectDone();
    }
}
