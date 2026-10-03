package com.github.lisama569.idempotenttaskqueuejava.persistence;

/**
 * 恢复时发现无法自洽的持久化状态（例如位点越过了未终态任务），
 * 拒绝启动并说明卡点，绝不悄悄糊过去。
 */
public class RecoveryException extends RuntimeException {
    public RecoveryException(String message) {
        super(message);
    }

    public RecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
