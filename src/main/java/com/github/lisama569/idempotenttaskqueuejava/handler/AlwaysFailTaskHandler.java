package com.github.lisama569.idempotenttaskqueuejava.handler;

import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;

/** 始终失败的演示处理器，用于验证重试耗尽进入 FAILED 终态。 */
public class AlwaysFailTaskHandler implements TaskHandler {

    @Override
    public String type() {
        return "demo.always-fail";
    }

    @Override
    public void handle(TaskRecord task) throws Exception {
        throw new IllegalStateException("演示任务恒定失败, businessKey=" + task.getBusinessKey());
    }
}
