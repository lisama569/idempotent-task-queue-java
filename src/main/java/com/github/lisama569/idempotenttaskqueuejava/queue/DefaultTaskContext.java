package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;

/**
 * 默认执行上下文：包装当前被领取的任务快照与本次尝试序号。
 * sideEffectDone 直接读写 TaskRecord 中的幂等标记（该字段在崩溃重投场景仍保留）。
 */
public class DefaultTaskContext implements TaskContext {
    private final TaskRecord task;
    private final int attempt;
    /** 副作用标记落盘钩子（纯内存模式为 null）：先落盘再置内存标记。 */
    private final Runnable sideEffectPersistHook;

    public DefaultTaskContext(TaskRecord task, int attempt) {
        this(task, attempt, null);
    }

    public DefaultTaskContext(TaskRecord task, int attempt, Runnable sideEffectPersistHook) {
        this.task = task;
        this.attempt = attempt;
        this.sideEffectPersistHook = sideEffectPersistHook;
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
        if (task.isSideEffectDone()) {
            return;
        }
        // 先落盘再置内存标记：若进程在两者之间崩溃，恢复后仍是“已标记”，重投跳过副作用
        if (sideEffectPersistHook != null) {
            sideEffectPersistHook.run();
        }
        task.markSideEffectDone();
    }

    @Override
    public boolean sideEffectDone() {
        return task.isSideEffectDone();
    }
}
