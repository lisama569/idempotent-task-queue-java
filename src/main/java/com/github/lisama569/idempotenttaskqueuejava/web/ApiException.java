package com.github.lisama569.idempotenttaskqueuejava.web;

/**
 * 控制器层业务异常，携带可区分的机器可读错误码。
 */
public class ApiException extends RuntimeException {

    private final String code;
    private final int httpStatus;

    public ApiException(String code, String message, int httpStatus) {
        super(message);
        this.code = code;
        this.httpStatus = httpStatus;
    }

    public String getCode() { return code; }
    public int getHttpStatus() { return httpStatus; }
}
