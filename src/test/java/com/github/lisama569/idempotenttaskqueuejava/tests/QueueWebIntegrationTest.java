package com.github.lisama569.idempotenttaskqueuejava.tests;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/**
 * 端到端集成测试：真实 Spring 容器 + 自动消费 worker，覆盖
 * 接受(201)、重复(409/DUPLICATE_BUSINESS_KEY)、非法(400/INVALID_TASK) 与统计查询。
 */
@SpringBootTest
@TestPropertySource(properties = {
        "idempotent.queue.capacity=50",
        "idempotent.queue.rate-limit-per-second=10000",
        "idempotent.queue.max-attempts=2",
        "idempotent.queue.retry-backoff-millis=10",
        "idempotent.queue.delivery-timeout-millis=200",
        "idempotent.queue.auto-start-worker=true"
})
class QueueWebIntegrationTest {
    private static final Logger log = LoggerFactory.getLogger(QueueWebIntegrationTest.class);

    @Autowired
    private WebApplicationContext wac;

    private MockMvc mvc() {
        return MockMvcBuilders.webAppContextSetup(wac).build();
    }

    @Test
    void enqueue_duplicate_invalid_andStats_areDistinguishable() throws Exception {
        MockMvc mvc = mvc();
        String key = "web-" + System.nanoTime();

        log.info("[TEST-INPUT] POST /api/queue/tasks businessKey={}", key);
        mvc.perform(post("/api/queue/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"businessKey\":\"" + key + "\",\"type\":\"email\",\"payload\":\"hi\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.accepted").value(true))
                .andExpect(jsonPath("$.taskId").exists());

        // 重复投递：409 + 明确错误码 + 既有任务ID
        log.info("[TEST-INPUT] duplicate POST same businessKey={}", key);
        mvc.perform(post("/api/queue/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"businessKey\":\"" + key + "\",\"type\":\"email\",\"payload\":\"hi\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.accepted").value(false))
                .andExpect(jsonPath("$.errorCode").value("DUPLICATE_BUSINESS_KEY"))
                .andExpect(jsonPath("$.capacityReject").value(false))
                .andExpect(jsonPath("$.existingTaskId").exists());

        // 业务校验失败：400，错误码可区分
        mvc.perform(post("/api/queue/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"businessKey\":\"\",\"type\":\"email\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.errorCode").value("INVALID_TASK"));

        // 等待自动消费完成并查询
        Thread.sleep(300);
        mvc.perform(get("/api/queue/stats"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.workerRunning").value(true))
                .andExpect(jsonPath("$.acceptedTotal").value(1))
                .andExpect(jsonPath("$.rejectedDuplicateTotal").value(1))
                .andExpect(jsonPath("$.rejectedInvalidTotal").value(1))
                .andDo(r -> log.info("[TEST-RESULT] stats={}", r.getResponse().getContentAsString()));
        log.info("[TEST-BASIS] HTTP 409/400/429 与 errorCode 双重区分拒绝原因；stats 暴露输入/位点/判定依据");
    }
}
