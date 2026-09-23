package com.github.lisama569.idempotenttaskqueuejava.handler;

import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 前两次失败、第三次成功的演示处理器（按任务 ID 记忆），用于验证失败后重试成功。
 */
public class FlakyTaskHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(FlakyTaskHandler.class);

    private final Map<String, Integer> attemptsById = new ConcurrentHashMap<>();

    @Override
    public String type() {
        return "demo.flaky";
    }

    @Override
    public void handle(TaskRecord task) throws Exception {
        int n = attemptsById.merge(task.getId(), 1, Integer::sum);
        if (n < 3) {
            throw new IllegalStateException("演示任务第 " + n + " 次执行失败(预期内)");
        }
        log.info("[副作用] flaky 任务第 {} 次执行成功 seq={}", n, task.getSeq());
    }
}
