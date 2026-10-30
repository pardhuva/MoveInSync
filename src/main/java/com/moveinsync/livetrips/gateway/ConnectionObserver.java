package com.moveinsync.livetrips.gateway;

import com.moveinsync.livetrips.model.TripEvent;

/**
 * Hook for instrumenting a connection's delivery path.
 *
 * <p>Kept as an interface rather than a direct dependency on the metrics component so
 * {@link ClientConnection} stays free of Micrometer and remains unit-testable; tests pass a
 * recording observer and assert exactly what reached the wire.
 *
 * <p>{@link #onDelivered} fires after a frame is successfully written, which is the only honest
 * place to measure delivery latency &mdash; measuring at enqueue time would hide precisely the
 * back-pressure this system exists to manage.
 */
public interface ConnectionObserver {

    ConnectionObserver NOOP = new ConnectionObserver() { };

    /** An event was written to the socket. */
    default void onDelivered(TripEvent event) { }

    /** A socket write failed; the connection is being closed. */
    default void onWriteFailure() { }

    /** An SOS was flushed through the dedicated express drain path. */
    default void onSosExpressDelivery() { }
}
