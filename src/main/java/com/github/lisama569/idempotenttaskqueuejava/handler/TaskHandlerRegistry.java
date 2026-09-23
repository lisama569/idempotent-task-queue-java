package com.github.lisama569.idempotenttaskqueuejava.handler;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 任务类型 -&gt; 处理器 的注册表。
 */
public class TaskHandlerRegistry {

    private final Map<String, TaskHandler> handlers = new ConcurrentHashMap<>();

    /** 注册处理器；同一类型重复注册将被拒绝以暴露配置错误。 */
    public void register(TaskHandler handler) {
        TaskHandler previous = handlers.putIfAbsent(handler.type(), handler);
        if (previous != null) {
            throw new IllegalStateException("任务类型 " + handler.type() + " 的处理器已注册");
        }
    }

    public Optional<TaskHandler> find(String type) {
        return Optional.ofNullable(handlers.get(type));
    }
}
