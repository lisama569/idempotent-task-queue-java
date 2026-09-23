package com.github.lisama569.idempotenttaskqueuejava.exception;

/** 业务校验失败：任务字段不合法。 */
public class TaskValidationException extends QueueException {
    public TaskValidationException(String message) { super(message); }
}
