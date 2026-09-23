package com.github.lisama569.idempotenttaskqueuejava.handler;

import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;

/**
 * 业务任务处理器。
 *
 * <p>语义要求：在至少一次投递（进程崩溃恢复可能重复执行）的前提下，
 * 处理器自身的副作用应当是幂等的，框架保证同进程正常运行期间每个任务恰好执行一次。
 */
public interface TaskHandler {

    /** 该处理器处理的任务类型。 */
    String type();

    /**
     * 执行任务。抛异常视为业务失败，将按重试策略重试。
     */
    void handle(TaskRecord task) throws Exception;
}
