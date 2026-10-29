package com.moveinsync.livetrips.model;

/**
 * The kind of update flowing through the system. The type drives three behaviours
 * in the delivery pipeline:
 *
 * <ul>
 *   <li><b>priority</b> &mdash; higher value is delivered first (SOS &gt; STATUS &gt; LOCATION).</li>
 *   <li><b>conflatable</b> &mdash; if the consumer is behind, older values of this type for the
 *       same cab may be replaced by the latest (only LOCATION).</li>
 *   <li><b>critical</b> &mdash; must never be silently dropped (STATUS and SOS).</li>
 * </ul>
 */
public enum EventType {

    /** Periodic cab GPS position. High volume, safe to conflate to the latest per cab. */
    LOCATION(0, true, false),

    /** Trip lifecycle change (ASSIGNED, STARTED, DELAYED, COMPLETED, CANCELLED). Never dropped. */
    STATUS(1, false, true),

    /** Panic / SOS alert. Highest priority, never dropped, target end-to-end latency < 2s. */
    SOS(2, false, true),

    /** Server-generated full-state snapshot sent when a client has fallen outside the replay window. */
    SNAPSHOT(1, false, true);

    private final int priority;
    private final boolean conflatable;
    private final boolean critical;

    EventType(int priority, boolean conflatable, boolean critical) {
        this.priority = priority;
        this.conflatable = conflatable;
        this.critical = critical;
    }

    /** Higher wins. Used by the outbound queue's priority lanes. */
    public int priority() {
        return priority;
    }

    /** True only for LOCATION: older values may be replaced by the latest when a consumer lags. */
    public boolean isConflatable() {
        return conflatable;
    }

    /** True for STATUS/SOS/SNAPSHOT: the pipeline must preserve these, never silently discard them. */
    public boolean isCritical() {
        return critical;
    }
}
