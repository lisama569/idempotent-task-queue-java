package com.github.lisama569.idempotenttaskqueuejava.handler;

import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** 始终成功的演示处理器。 */
public class SuccessTaskHandler implements TaskHandler {

    private static final Logger log = LoggerFactory.getLogger(SuccessTaskHandler.class);

    @Override
    public String type() {
        return "demo.success";
    }

    @Override
    public void handle(TaskRecord task) {
        log.info("[副作用] 处理成功任务 seq={} businessKey={} payload={}",
                task.getSeq(), task.getBusinessKey(), task.getPayload());
    }
}
