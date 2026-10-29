package com.moveinsync.livetrips.gateway;

/**
 * The narrow seam between the delivery pipeline and the actual socket.
 *
 * <p>Everything interesting &mdash; back-pressure, conflation, priority, resume &mdash; is
 * implemented against this interface rather than against a {@code WebSocketSession}. That is
 * what makes the pipeline testable in-process: tests substitute a recording transport (and a
 * deliberately slow or failing one) with no servlet container, no network and no Docker.
 */
public interface ClientTransport {

    /** Stable id for this transport (the WebSocket session id in production). */
    String id();

    /**
     * Write one text frame. May block if the peer is slow; callers must not hold locks that
     * other connections need.
     *
     * @throws Exception if the write fails; the caller closes the connection
     */
    void send(String payload) throws Exception;

    /** Close the connection. Implementations must be idempotent. */
    void close(String reason);

    boolean isOpen();
}
