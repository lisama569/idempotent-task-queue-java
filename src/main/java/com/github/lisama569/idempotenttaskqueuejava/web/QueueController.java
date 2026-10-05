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

    /** 手动触发历史收拢：全量状态写入快照并清空日志。纯内存模式返回 enabled=false。 */
    @PostMapping("/compact")
    public Map<String, Object> compact() {
        var report = queue.compact();
        Map<String, Object> body = new LinkedHashMap<>();
        if (report == null) {
            body.put("enabled", false);
            body.put("detail", "persistence disabled; nothing to compact");
            return body;
        }
        body.put("enabled", true);
        body.put("tasksSnapshotted", report.tasksSnapshotted());
        body.put("journalBytesBefore", report.journalBytesBefore());
        body.put("journalBytesAfter", report.journalBytesAfter());
        body.put("committedOffset", report.committedOffset());
        return body;
    }

    /** 最近一次启动时的恢复报告（进度/位点/判断依据）。纯内存模式返回 enabled=false。 */
    @GetMapping("/recovery")
    public Map<String, Object> recovery() {
        var report = queue.lastRecovery();
        Map<String, Object> body = new LinkedHashMap<>();
        if (report == null) {
            body.put("enabled", false);
            return body;
        }
        body.put("enabled", true);
        body.put("snapshotLoaded", report.snapshotLoaded());
        body.put("snapshotCommittedOffset", report.snapshotCommittedOffset());
        body.put("snapshotTaskCount", report.snapshotTaskCount());
        body.put("journalRecordsReplayed", report.journalRecordsReplayed());
        body.put("journalEventsSkipped", report.journalEventsSkipped());
        body.put("journalTruncatedAtBytes", report.journalTruncatedAtBytes());
        body.put("journalTornTailBytes", report.journalTornTailBytes());
        body.put("tasksRecovered", report.tasksRecovered());
        body.put("committedOffset", report.committedOffset());
        body.put("sequence", report.sequence());
        body.put("basis", report.basis());
        return body;
    }
}
