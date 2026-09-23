package com.github.lisama569.idempotenttaskqueuejava.exception;

import com.github.lisama569.idempotenttaskqueuejava.domain.OffsetRejectReason;

/** 位点提交被拒绝（回退或乱序）。 */
public class OffsetRejectedException extends QueueException {
    private final long requested;
    private final long current;
    private final OffsetRejectReason reason;

    public OffsetRejectedException(long requested, long current, OffsetRejectReason reason) {
        super("offset commit rejected: requested=" + requested + ", current=" + current
                + ", reason=" + reason);
        this.requested = requested;
        this.current = current;
        this.reason = reason;
    }

    public long getRequested() { return requested; }
    public long getCurrent() { return current; }
    public OffsetRejectReason getReason() { return reason; }
}
