package com.github.lisama569.idempotenttaskqueuejava.web;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.Map;

/**
 * 统一异常响应：错误码与 HTTP 状态分离，确保
 * DUPLICATE_BUSINESS_KEY / CAPACITY_EXCEEDED / RATE_LIMITED / VALIDATION_FAILED
 * 在响应中可区分。
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<Map<String, Object>> handleApi(ApiException ex) {
        log.info("[拒绝] code={} message={}", ex.getCode(), ex.getMessage());
        return ResponseEntity.status(ex.getHttpStatus()).body(Map.of(
                "accepted", false,
                "reason", ex.getCode(),
                "message", ex.getMessage()
        ));
    }
}
