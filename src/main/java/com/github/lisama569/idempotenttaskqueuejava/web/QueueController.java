package com.github.lisama569.idempotenttaskqueuejava.web;

import com.github.lisama569.idempotenttaskqueuejava.domain.EnqueueResult;
import com.github.lisama569.idempotenttaskqueuejava.domain.QueueStats;
import com.github.lisama569.idempotenttaskqueuejava.domain.RejectReason;
import com.github.lisama569.idempotenttaskqueuejava.domain.TaskView;
import com.github.lisama569.idempotenttaskqueuejava.exception.TaskNotFoundException;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 队列 REST 接口：
 * 投递接受返回 201；重复/业务校验/容量/速率拒绝返回 200 accepted=false 且 errorCode 可区分。
 */
@RestController
@RequestMapping("/api/queue")
public class QueueController {

    private final IdempotentTaskQueue queue;

    public QueueController(IdempotentTaskQueue queue) {
        this.queue = queue;
    }

    @PostMapping("/tasks")
    public ResponseEntity<Map<String, Object>> enqueue(@RequestBody EnqueueRequest request) {
        EnqueueResult result = queue.enqueue(
                request.businessKey() == null ? null : request.businessKey().trim(),
                request.type(),
                request.payload());

        Map<String, Object> body = new LinkedHashMap<>();
        body.put("accepted", result.isAccepted());
        if (result.isAccepted()) {
            body.put("taskId", result.getTaskId());
            body.put("committedOffset", queue.committedOffset());
            return ResponseEntity.status(HttpStatus.CREATED).body(body);
        }
        body.put("errorCode", result.getRejectReason().name());
        body.put("capacityReject", result.getRejectReason().isCapacityReason());
        body.put("existingTaskId", result.getExistingTaskId());
        body.put("detail", result.getDetail());
        // 业务校验 400；重复键 409；容量/速率 429（限流）语义，彼此可区分
        HttpStatus status = switch (result.getRejectReason()) {
            case INVALID_TASK -> HttpStatus.BAD_REQUEST;
            case DUPLICATE_BUSINESS_KEY -> HttpStatus.CONFLICT;
            case QUEUE_FULL, RATE_LIMITED -> HttpStatus.TOO_MANY_REQUESTS;
        };
        return ResponseEntity.status(status).body(body);
    }

    @GetMapping("/tasks")
    public List<TaskView> list() {
        return queue.listTasks();
    }

    @GetMapping("/tasks/{id}")
    public TaskView get(@PathVariable long id) {
        TaskView view = queue.getTask(id);
        if (view == null) {
            throw new TaskNotFoundException(id);
        }
        return view;
    }

    @GetMapping("/stats")
    public QueueStats stats() {
        return queue.stats();
    }
}
