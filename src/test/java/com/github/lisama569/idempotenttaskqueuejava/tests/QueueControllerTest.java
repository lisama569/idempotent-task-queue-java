package com.github.lisama569.idempotenttaskqueuejava.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.idempotenttaskqueuejava.config.QueueProperties;
import com.github.lisama569.idempotenttaskqueuejava.handler.AlwaysFailTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.FlakyTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.SuccessTaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.TaskHandlerRegistry;
import com.github.lisama569.idempotenttaskqueuejava.queue.ConsumerLoop;
import com.github.lisama569.idempotenttaskqueuejava.queue.IdempotentTaskQueue;
import com.github.lisama569.idempotenttaskqueuejava.retry.FixedBackoffRetryPolicy;
import com.github.lisama569.idempotenttaskqueuejava.storage.WalStore;
import com.github.lisama569.idempotenttaskqueuejava.web.GlobalExceptionHandler;
import com.github.lisama569.idempotenttaskqueuejava.web.QueueController;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.nio.file.Path;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * HTTP 层测试：验证拒绝原因与状态码可区分，位点回退被明确拒绝。
 */
class QueueControllerTest {

    @TempDir
    Path tempDir;

    private MockMvc mvc;
    private final ObjectMapper json = new ObjectMapper();
    private IdempotentTaskQueue queue;
    private ConsumerLoop consumer;

    @BeforeEach
    void setUp() {
        TaskHandlerRegistry registry = new TaskHandlerRegistry();
        registry.register(new SuccessTaskHandler());
        registry.register(new AlwaysFailTaskHandler());
        registry.register(new FlakyTaskHandler());
        queue = new IdempotentTaskQueue(
                new WalStore(tempDir.resolve("http.wal")), registry,
                new FixedBackoffRetryPolicy(10), 2, 100);
        queue.recover();
        consumer = new ConsumerLoop(queue, registry, 5);
        mvc = MockMvcBuilders.standaloneSetup(new QueueController(queue, consumer))
                .setControllerAdvice(new GlobalExceptionHandler())
                .build();
    }

    @Test
    void submitDuplicateReturnsDistinguishableConflict() throws Exception {
        var body = Map.of("businessKey", "http-1", "type", "demo.success",
                "payload", "{}", "maxAttempts", 3);
        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(body)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.seq").value(1));

        // 重复投递：409 + DUPLICATE_BUSINESS_KEY，与 400/429 可区分
        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(body)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.reason").value("DUPLICATE_BUSINESS_KEY"));

        // 校验失败：400 + VALIDATION_FAILED
        var invalid = Map.of("businessKey", " ", "type", "demo.success");
        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(invalid)))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.reason").value("VALIDATION_FAILED"));
    }

    @Test
    void capacityExceededReturns429WithReason() throws Exception {
        for (int i = 1; i <= 2; i++) {
            mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                            .content(json.writeValueAsString(Map.of("businessKey", "cap-" + i,
                                    "type", "demo.success"))))
                    .andExpect(status().isOk());
        }
        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("businessKey", "cap-3",
                                "type", "demo.success"))))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.reason").value("CAPACITY_EXCEEDED"));
    }

    @Test
    void snapshotAndOffsetRollbackAreObservable() throws Exception {
        mvc.perform(get("/api/queue"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.committedOffset").value(0))
                .andExpect(jsonPath("$.capacity").value(2));

        // 位点回退被拒绝
        mvc.perform(post("/api/queue/offset").contentType(MediaType.APPLICATION_JSON)
                        .content(json.writeValueAsString(Map.of("offset", -1))))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.rejected").value(true))
                .andExpect(jsonPath("$.reason").value("ROLLBACK"));
    }
}
