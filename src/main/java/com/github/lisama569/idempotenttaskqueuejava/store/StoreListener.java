package com.github.lisama569.idempotenttaskqueuejava.store;

import com.github.lisama569.idempotenttaskqueuejava.domain.TaskRecord;

/**
 * TaskStore 状态变更监听：所有回调都在 TaskStore 的同一把锁内触发，
 * 因此监听到的事件流与内存状态变更严格同序。
 * 默认空实现（纯内存模式不挂监听器时无任何开销语义变化）。
 */
public interface StoreListener {

    StoreListener NO_OP = new StoreListener() {
    };

    /** 新任务被接受并插入后触发。 */
    default void onEnqueued(TaskRecord record, long acceptedTotal) {
    }

    /** 任务可变状态（status/attempts/nextVisibleAt 等）发生变化后触发。 */
    default void onStatusChanged(TaskRecord record) {
    }

    /** 业务键重复拒绝后触发（携带累计拒绝数）。 */
    default void onDuplicateRejected(long rejectedDuplicateTotal) {
    }

    /** 容量拒绝后触发（携带累计拒绝数）。 */
    default void onCapacityRejected(long rejectedCapacityTotal) {
    }
}
