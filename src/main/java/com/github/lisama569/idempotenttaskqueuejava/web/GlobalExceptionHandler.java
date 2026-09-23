package com.github.lisama569.idempotenttaskqueuejava.web;

import com.github.lisama569.idempotenttaskqueuejava.exception.OffsetRejectedException;
import com.github.lisama569.idempotenttaskqueuejava.exception.TaskNotFoundException;
import com.github.lisama569.idempotenttaskqueuejava.exception.TaskValidationException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/** 异常到 HTTP 响应的映射，错误码与投递拒绝原因可区分。 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(TaskValidationException.class)
    public ResponseEntity<ApiError> handleValidation(TaskValidationException e) {
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(new ApiError("INVALID_TASK", e.getMessage()));
    }

    @ExceptionHandler(TaskNotFoundException.class)
    public ResponseEntity<ApiError> handleNotFound(TaskNotFoundException e) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(new ApiError("TASK_NOT_FOUND", e.getMessage()));
    }

    @ExceptionHandler(OffsetRejectedException.class)
    public ResponseEntity<ApiError> handleOffset(OffsetRejectedException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(new ApiError("OFFSET_" + e.getReason().name(), e.getMessage()));
    }
}
