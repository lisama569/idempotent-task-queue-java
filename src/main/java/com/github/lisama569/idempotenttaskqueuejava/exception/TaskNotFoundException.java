package com.github.lisama569.idempotenttaskqueuejava.exception;

/** 查询或操作的任务不存在。 */
public class TaskNotFoundException extends QueueException {
    public TaskNotFoundException(long taskId) {
        super("task not found: " + taskId);
    }
}
