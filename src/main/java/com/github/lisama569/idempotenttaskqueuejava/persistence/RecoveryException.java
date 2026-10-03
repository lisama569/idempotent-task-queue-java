package com.github.lisama569.idempotenttaskqueuejava.persistence;

/**
 * 恢复失败：本地存储出现无法安全修复的矛盾（如快照校验失败、日志中段损坏），
 * 明确拒绝启动，并说明卡在哪、判断依据是什么。
 */
public class RecoveryException extends RuntimeException {
    public RecoveryException(String message) {
        super(message);
    }

    public RecoveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
