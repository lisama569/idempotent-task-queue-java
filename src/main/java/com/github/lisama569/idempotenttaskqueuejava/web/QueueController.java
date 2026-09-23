package com.github.lisama569.idempotenttaskqueuejava.web;

import com.github.lisama569.idempotenttaskqueuejava.model.OffsetCommitResult;
import com.github.lisama569.idempotenttaskqueuejava.model.QueueSnapshot;
import com.github.lisama569.idempotenttaskqueuejava.model.SubmitResult;
import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;
import com.github.lisama569.idempotenttaskqueuejava.queue.ConsumerLoop;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * HTTP 接口：
 * <ul>
 *   <li>POST /api/tasks 投递</li>
 *   <li>GET  /api/tasks/{businessKey} 查单任务</li>
 *   <li>GET  /api/queue 队列快照（位点、容量、计数）</li>
 *   <li>POST /api/queue/offset 提交位点（用于演示回退拒绝）</li>
 *   <li>POST /api/consumer/start|stop 消费重启演示</li>
 * </ul>
 */
@RestController
@RequestMapping("/api")
public class QueueController {

    private static final Logger log = LoggerFactory.getLogger(QueueController.class);

    private final IdempotentTaskQueue queue;
    private final ConsumerLoop consumer;

    public QueueController(IdempotentTaskQueue queue, ConsumerLoop consumer) {
        this.queue = queue;
        this.consumer = consumer;
    }

    /** 投递请求体。 */
    public record SubmitRequest(String businessKey, String type, String payload, Integer maxAttempts) {
    }

    @PostMapping("/tasks")
    public Map<String, Object> submit(@RequestBody SubmitRequest req) {
        log.info("[投递输入] businessKey={} type={} maxAttempts={}",
                req == null ? null : req.businessKey(),
                req == null ? null : req.type(),
                req == null ? null : req.maxAttempts());
        SubmitResult result = queue.submit(
                req == null ? null : req.businessKey(),
                req == null ? null : req.type(),
                req == null ? null : req.payload(),
                req == null ? null : req.maxAttempts());
        log.info("[投递判定] accepted={} reason={} seq={} 依据={}",
                result.accepted(), result.reason(), result.seq(), result.message());
        if (!result.accepted()) {
            HttpStatus status = switch (result.reason()) {
                case VALIDATION_FAILED -> HttpStatus.BAD_REQUEST;            // 400
                case DUPLICATE_BUSINESS_KEY -> HttpStatus.CONFLICT;          // 409
                case CAPACITY_EXCEEDED, RATE_LIMITED -> HttpStatus.TOO_MANY_REQUESTS; // 429
            };
            throw new ApiException(result.reason().name(), result.message(), status.value());
        }
        return Map.of(
                "accepted", true,
                "taskId", result.taskId(),
                "seq", result.seq(),
                "message", result.message()
        );
    }

    @GetMapping("/tasks/{businessKey}")
    public TaskRecord getTask(@PathVariable String businessKey) {
        return queue.getByBusinessKey(businessKey)
                .orElseThrow(() -> new ApiException("TASK_NOT_FOUND",
                        "任务不存在: " + businessKey, HttpStatus.NOT_FOUND.value()));
    }

    @GetMapping("/queue")
    public QueueSnapshot snapshot() {
        return queue.snapshot();
    }

    /** 位点提交请求体。 */
    public record OffsetRequest(Long offset) {
    }

    @PostMapping("/queue/offset")
    public OffsetCommitResult commitOffset(@RequestBody OffsetRequest req) {
        if (req == null || req.offset() == null) {
            throw new ApiException("VALIDATION_FAILED", "offset 不能为空",
                    HttpStatus.BAD_REQUEST.value());
        }
        log.info("[位点输入] 请求位点={} 当前位点={}", req.offset(), queue.committedOffset());
        OffsetCommitResult r = queue.commitOffset(req.offset());
        log.info("[位点判定] committed={} rejected={} reason={} 依据={}",
                r.committed(), r.rejected(), r.reason(), r.message());
        return r;
    }

    @PostMapping("/consumer/{action}")
    public Map<String, Object> consumerControl(@PathVariable String action) {
        switch (action) {
            case "stop" -> {
                consumer.stop();
                return Map.of("running", consumer.isRunning(),
                        "committedOffset", queue.committedOffset());
            }
            case "start" -> {
                consumer.start();
                return Map.of("running", consumer.isRunning(),
                        "committedOffset", queue.committedOffset());
            }
            default -> throw new ApiException("VALIDATION_FAILED",
                    "未知 action: " + action + "（只支持 start/stop）",
                    HttpStatus.BAD_REQUEST.value());
        }
    }
}
