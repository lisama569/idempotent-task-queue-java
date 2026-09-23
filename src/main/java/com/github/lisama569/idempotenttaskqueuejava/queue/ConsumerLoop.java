package com.github.lisama569.idempotenttaskqueuejava.queue;

import com.github.lisama569.idempotenttaskqueuejava.handler.TaskHandler;
import com.github.lisama569.idempotenttaskqueuejava.handler.TaskHandlerRegistry;
import com.github.lisama569.idempotenttaskqueuejava.model.TaskRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 单线程消费循环。
 *
 * <p>不变量：
 * <ul>
 *   <li>同一时刻只有一个任务在执行，正常运行期内副作用不会重复执行；</li>
 *   <li>严格按序号连续消费，任务失败重试期间排在其后的任务不被跳过（顺序可观察）；</li>
 *   <li>{@link #stop()} 为优雅停止：等待当前任务执行并落盘后退出，
 *       随后 {@link #start()} 即模拟“消费进程重启”，从 WAL 恢复的位点继续，不丢不重。</li>
 * </ul>
 */
public class ConsumerLoop {

    private static final Logger log = LoggerFactory.getLogger(ConsumerLoop.class);

    private final IdempotentTaskQueue queue;
    private final TaskHandlerRegistry registry;
    private final long pollMillis;
    private final AtomicLong executionCounter = new AtomicLong();

    private Thread worker;
    private volatile boolean running;

    public ConsumerLoop(IdempotentTaskQueue queue, TaskHandlerRegistry registry, long pollMillis) {
        this.queue = queue;
        this.registry = registry;
        this.pollMillis = pollMillis;
    }

    /** 启动消费线程（幂等：已在运行则不重复启动）。 */
    public synchronized void start() {
        if (running) {
            return;
        }
        running = true;
        worker = new Thread(this::runLoop, "idempotent-queue-consumer");
        worker.setDaemon(true);
        worker.start();
        log.info("消费者已启动");
    }

    /** 优雅停止：等待当前任务执行完成并落盘后返回。 */
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        Thread t = worker;
        if (t != null) {
            try {
                t.join(10_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        worker = null;
        log.info("消费者已优雅停止，当前位点={}", queue.committedOffset());
    }

    public boolean isRunning() {
        return running;
    }

    public long executionCount() {
        return executionCounter.get();
    }

    /**
     * 模拟消费进程崩溃：不再消费新任务，且不等待当前执行中的任务完成（不写终态日志）。
     * 仅供测试使用。崩溃时当前任务在 WAL 中停留在 RUNNING，新进程 recover() 后复位为 PENDING。
     * 该方法不中断执行线程：被放弃的线程若稍后自行结束，也不允许再触碰（已恢复的）队列，
     * 因此测试崩溃恢复时会用一个新的队列实例。
     */
    public synchronized void abandonForCrashSimulation() {
        running = false;
        worker = null;
        log.warn("模拟消费进程崩溃：放弃当前执行线程，位点={}", queue.committedOffset());
    }

    private void runLoop() {
        while (running) {
            try {
                Optional<TaskRecord> claimed = queue.claimNext();
                if (claimed.isEmpty()) {
                    sleepQuiet(pollMillis);
                    continue;
                }
                executeOne(claimed.get());
            } catch (Exception e) {
                log.error("消费循环异常", e);
                sleepQuiet(pollMillis);
            }
        }
    }

    private void executeOne(TaskRecord task) {
        long offsetBefore = queue.committedOffset();
        Optional<TaskHandler> handler = registry.find(task.getType());
        executionCounter.incrementAndGet();
        log.info("[消费输入] seq={} businessKey={} type={} attempt={}/{} 提交位点={}",
                task.getSeq(), task.getBusinessKey(), task.getType(),
                task.getAttempts(), task.getMaxAttempts(), offsetBefore);
        try {
            if (handler.isEmpty()) {
                throw new IllegalStateException("无对应处理器: " + task.getType());
            }
            handler.get().handle(task);
            queue.reportSuccess(task);
            log.info("[消费判定] seq={} 结果=SUCCEEDED 提交位点={} -> {}",
                    task.getSeq(), offsetBefore, queue.committedOffset());
        } catch (Exception e) {
            queue.reportFailure(task, e);
            TaskRecord after = queue.getBySeq(task.getSeq()).orElse(task);
            log.info("[消费判定] seq={} 结果={} attempts={}/{} 提交位点={} 判定依据={}",
                    task.getSeq(), after.getStatus(), after.getAttempts(),
                    after.getMaxAttempts(), queue.committedOffset(),
                    after.getStatus().name().equals("FAILED")
                            ? "重试次数已达上限，进入 FAILED 终态: " + after.getLastError()
                            : "失败但未超过上限，退避后重试: " + after.getLastError());
        }
    }

    private void sleepQuiet(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
