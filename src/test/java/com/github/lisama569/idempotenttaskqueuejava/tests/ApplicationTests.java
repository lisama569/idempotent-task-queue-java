package com.github.lisama569.idempotenttaskqueuejava.tests;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;

/**
 * 装配冒烟测试：关闭自动消费，仅验证上下文可启动。
 */
@SpringBootTest
@TestPropertySource(properties = "idempotent.queue.auto-start-worker=false")
public class ApplicationTests {
    @Test
    public void contentLoad() {
    }
}
