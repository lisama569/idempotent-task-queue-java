package com.github.lisama569.idempotenttaskqueuejava.exception;

/** 队列模块受检语义异常基类（运行时抛出）。 */
public class QueueException extends RuntimeException {
    public QueueException(String message) { super(message); }
}
