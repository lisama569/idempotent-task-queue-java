package com.github.lisama569.idempotenttaskqueuejava.domain;

/** 位点提交被拒绝的原因。 */
public enum OffsetRejectReason {
    /** 请求位点小于已提交位点：回退被拒绝。 */
    REGRESSION,
    /** 请求位点跳过了尚未完成的相邻任务：乱序缺口被拒绝。 */
    GAP
}
