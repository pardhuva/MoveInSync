package com.moveinsync.livetrips.gateway;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.core.OutboundQueue;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

/**
 * One connected subscriber: its identity, its subscriptions, its bounded
 * {@link OutboundQueue}, and the single serialised writer that drains that queue to the socket.
 *
 * <h2>Why there is no thread per connection</h2>
 * 200,000 connections cannot have 200,000 threads. Instead a connection is <i>drain-scheduled</i>:
 * when an event is offered, the connection submits a drain task to a shared writer pool, but only
 * if it does not already have one in flight ({@link #draining} compare-and-set). So a connection
 * occupies a pool thread only while it actually has bytes to write, at most one thread at a time,
 * which both bounds thread usage and guarantees writes are serialised and ordered.
 *
 * <h2>Why writes take a lock</h2>
 * The subscribe path must emit {@code subscribed} + snapshot/replay <i>before</i> any live event
 * for that topic reaches the wire. It therefore holds {@link #writeLock} across the whole setup
 * while live events accumulate in the queue. Drain tasks for this connection wait on the same
 * lock. The lock is per connection, so a slow or mid-subscribe client never blocks another.
 *
 * <h2>Duplicates over gaps (deliberate)</h2>
 * The subscription is registered <i>before</i> the replay window is computed. An event published
 * in that instant can therefore be delivered twice &mdash; once from replay, once live &mdash; but
 * can never be missed. At-least-once plus client-side dedup by sequence turns that into
 * exactly-once effect; the reverse ordering would risk a silent gap, which nothing can repair.
 */
public class ClientConnection {

    private static final Logger log = LoggerFactory.getLogger(ClientConnection.class);

    /** Max frames one drain task writes before yielding its pool thread, so no client monopolises it. */
    private static final int DRAIN_BATCH_LIMIT = 256;

    private final ClientTransport transport;
    private final OutboundQueue queue;
    private final FrameCodec codec;
    private final Executor writerPool;
    /** Dedicated pool for flushing SOS, so an SOS never waits behind location drains on a saturated main pool. */
    private final Executor sosWriterPool;
    private final ConnectionObserver observer;

    private final ReentrantLock writeLock = new ReentrantLock();
    private final AtomicBoolean draining = new AtomicBoolean(false);
    /** Separate from {@link #draining}: an express SOS flush may be scheduled while a normal drain runs. */
    private final AtomicBoolean expressDraining = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile AuthenticatedPrincipal principal;
    private final Set<String> topics = ConcurrentHashMap.newKeySet();
    private final Map<String, Long> lastDeliveredSeq = new ConcurrentHashMap<>();

    private final long connectedAtMs = System.currentTimeMillis();
    private final AtomicLong writeFailures = new AtomicLong();
    /** Express flushes that gave up because a normal drain held the write lock. */
    private final AtomicLong expressLockContended = new AtomicLong();
    /** Express flushes that acquired the lock and wrote at least one SOS. */
    private final AtomicLong expressFlushes = new AtomicLong();
    private volatile String closeReason;

    /** Invoked exactly once when this connection closes, so the registry can clean up. */
    private volatile Consumer<ClientConnection> onClose = c -> { };

    public ClientConnection(ClientTransport transport, OutboundQueue queue,
                            FrameCodec codec, Executor writerPool) {
        this(transport, queue, codec, writerPool, writerPool, ConnectionObserver.NOOP);
    }

    public ClientConnection(ClientTransport transport, OutboundQueue queue,
                            FrameCodec codec, Executor writerPool, ConnectionObserver observer) {
        this(transport, queue, codec, writerPool, writerPool, observer);
    }

    /**
     * @param sosWriterPool dedicated pool for the express SOS flush path. May be the same instance
     *                      as {@code writerPool} (then express drains share the main pool and lose
     *                      their saturation advantage, but behaviour is otherwise identical).
     */
    public ClientConnection(ClientTransport transport, OutboundQueue queue, FrameCodec codec,
                            Executor writerPool, Executor sosWriterPool, ConnectionObserver observer) {
        this.transport = transport;
        this.queue = queue;
        this.codec = codec;
        this.writerPool = writerPool;
        this.sosWriterPool = sosWriterPool == null ? writerPool : sosWriterPool;
        this.observer = observer == null ? ConnectionObserver.NOOP : observer;
    }

    public String id() {
        return transport.id();
    }

    public AuthenticatedPrincipal principal() {
        return principal;
    }

    public void authenticate(AuthenticatedPrincipal principal) {
        this.principal = principal;
    }

    public boolean isAuthenticated() {
        return principal != null;
    }

    public Set<String> topics() {
        return Set.copyOf(topics);
    }

    public OutboundQueue queue() {
        return queue;
    }

    public boolean isOpen() {
        return !closed.get() && transport.isOpen();
    }

    public String closeReason() {
        return closeReason;
    }

    public long connectedAtMs() {
        return connectedAtMs;
    }

    public long expressLockContended() {
        return expressLockContended.get();
    }

    public long expressFlushes() {
        return expressFlushes.get();
    }

    public long writeFailures() {
        return writeFailures.get();
    }

    public void onClose(Consumer<ClientConnection> callback) {
        this.onClose = callback;
    }

    void addTopic(String topic) {
        topics.add(topic);
    }

    void removeTopic(String topic) {
        topics.remove(topic);
        lastDeliveredSeq.remove(topic);
    }

    /** The highest sequence written to this socket for a topic (observability / lag metric). */
    public long lastDeliveredSeq(String topic) {
        return lastDeliveredSeq.getOrDefault(topic, 0L);
    }

    /** Acquire the per-connection write lock. Used by the subscribe path; always paired with unlock. */
    public void lockWrites() {
        writeLock.lock();
    }

    public void unlockWrites() {
        writeLock.unlock();
        // Setup finished: anything that queued meanwhile must now be flushed.
        if (!queue.isEmpty()) {
            scheduleDrain();
        }
    }

    /**
     * Offer a live event. Never blocks. If the queue declares overflow (it cannot admit a
     * critical event without losing another), the connection is closed so the client can
     * reconnect and resume losslessly from the replay buffer.
     */
    public OutboundQueue.OfferOutcome offer(TripEvent event) {
        if (!isOpen()) {
            return OutboundQueue.OfferOutcome.REJECTED_OVERFLOW;
        }
        OutboundQueue.OfferOutcome outcome = queue.offer(event);
        if (outcome == OutboundQueue.OfferOutcome.REJECTED_OVERFLOW) {
            closeSlowConsumer();
        } else {
            // An SOS is flushed through the dedicated express path so it does not wait behind
            // location drains when the main writer pool is saturated. The normal drain is still
            // scheduled to flush the main lane; whichever path runs first removes the SOS exactly
            // once (both poll the same synchronized queue).
            if (event.type() == EventType.SOS) {
                scheduleExpressDrain();
            }
            scheduleDrain();
        }
        return outcome;
    }

    /** Schedule an express SOS flush on the dedicated pool, unless one is already in flight. */
    private void scheduleExpressDrain() {
        if (expressDraining.compareAndSet(false, true)) {
            try {
                sosWriterPool.execute(this::expressDrain);
            } catch (RuntimeException ex) {
                // Express pool saturated/rejected: fall back to the normal drain (its poll() returns
                // SOS first anyway). Release the flag so a later SOS can retry the express path.
                expressDraining.set(false);
            }
        }
    }

    /**
     * Flush only the SOS express lane to the socket. Kept short (SOS only) so it returns its
     * express-pool thread quickly.
     *
     * <p><b>Never blocks on the write lock.</b> It uses {@code tryLock}: if a normal drain already
     * holds the lock, that drain is itself writing this SOS (its {@code poll()} returns the express
     * lane first), so there is nothing to gain by waiting &mdash; and waiting is actively harmful.
     * Blocking here pins express-pool threads behind 256-frame batch writes, so under a heavy
     * backlog the whole (small) express pool parks on locks and SOS delivery starves. That is a
     * measured regression, not a theoretical one: blocking here pushed SOS p99 from ~475 ms to
     * ~10 s in the load simulation.
     *
     * <p>The SOS can never be stranded by giving up: {@link #offer} always schedules a normal drain
     * as well, and that drain polls the express lane first.
     */
    private void expressDrain() {
        boolean acquired = false;
        try {
            if (!isOpen()) {
                return;
            }
            acquired = writeLock.tryLock();
            if (!acquired) {
                expressLockContended.incrementAndGet();
                return; // a normal drain holds the lock and will deliver this SOS first
            }
            expressFlushes.incrementAndGet();
            try {
                TripEvent sos;
                while (isOpen() && (sos = queue.pollSos()) != null) {
                    try {
                        transport.send(codec.event(sos));
                        recordDelivered(sos);
                        observer.onSosExpressDelivery();
                    } catch (Exception ex) {
                        writeFailures.incrementAndGet();
                        observer.onWriteFailure();
                        log.debug("express write failed for connection {}: {}",
                                id(), ex.getClass().getSimpleName());
                        close("WRITE_FAILED");
                        return;
                    }
                }
            } finally {
                writeLock.unlock();
            }
        } finally {
            expressDraining.set(false);
            // Only re-arm if we actually held the lock and did work; re-arming after a failed
            // tryLock would spin the express pool against a lock we cannot get.
            if (acquired && isOpen() && queue.hasSos()) {
                scheduleExpressDrain();
            }
        }
    }

    /**
     * Write a pre-encoded frame immediately, under the write lock, bypassing the queue.
     * Used for control frames and for replay/snapshot during subscribe, where ordering with
     * respect to subsequent live events is established by holding the lock.
     */
    public boolean sendNow(String frame) {
        if (!isOpen()) {
            return false;
        }
        writeLock.lock();
        try {
            transport.send(frame);
            return true;
        } catch (Exception ex) {
            writeFailures.incrementAndGet();
            observer.onWriteFailure();
            // Never log frame contents: they can contain location payloads.
            log.debug("write failed for connection {}: {}", id(), ex.getClass().getSimpleName());
            close("WRITE_FAILED");
            return false;
        } finally {
            writeLock.unlock();
        }
    }

    /** Write an event immediately (used during replay), recording it as delivered. */
    public boolean sendEventNow(TripEvent event) {
        boolean ok = sendNow(codec.event(event));
        if (ok) {
            recordDelivered(event);
        }
        return ok;
    }

    /**
     * Send an event as part of a resume, marked {@code replay} so the client can tell a recovered
     * event from a live one (and so latency measurement does not count a recovered event's age,
     * which includes the client's offline time, against the live SOS target).
     */
    public boolean sendReplayEventNow(TripEvent event) {
        boolean ok = sendNow(codec.replayEvent(event));
        if (ok) {
            recordDelivered(event);
        }
        return ok;
    }

    private void recordDelivered(TripEvent event) {
        lastDeliveredSeq.merge(event.topic(), event.sequence(), Math::max);
        observer.onDelivered(event);
    }

    private void scheduleDrain() {
        if (draining.compareAndSet(false, true)) {
            try {
                writerPool.execute(this::drain);
            } catch (RuntimeException ex) {
                // Pool rejected (shutting down or saturated): release the flag so a later
                // offer can retry rather than leaving the connection permanently un-drained.
                draining.set(false);
                throw ex;
            }
        }
    }

    /** Drains up to a batch of events to the socket, then reschedules if more remain. */
    private void drain() {
        boolean more = false;
        try {
            writeLock.lock();
            try {
                int written = 0;
                TripEvent event;
                while (written < DRAIN_BATCH_LIMIT && (event = queue.poll()) != null) {
                    if (!isOpen()) {
                        return;
                    }
                    try {
                        transport.send(codec.event(event));
                        recordDelivered(event);
                    } catch (Exception ex) {
                        writeFailures.incrementAndGet();
                        observer.onWriteFailure();
                        log.debug("write failed for connection {}: {}", id(), ex.getClass().getSimpleName());
                        close("WRITE_FAILED");
                        return;
                    }
                    written++;
                }
                more = !queue.isEmpty();
            } finally {
                writeLock.unlock();
            }
        } finally {
            draining.set(false);
            // Re-check after clearing the flag: an offer that raced with us would have seen
            // draining==true and skipped scheduling, so we must pick its work up here.
            if (more || !queue.isEmpty()) {
                if (isOpen()) {
                    scheduleDrain();
                }
            }
        }
    }

    /**
     * The slow-consumer path: the client is so far behind that preserving a critical event is
     * impossible. Tell it why (best effort) and close. No status or SOS event is lost &mdash; the
     * client resumes from the shared replay buffer on reconnect.
     */
    public void closeSlowConsumer() {
        try {
            if (transport.isOpen()) {
                transport.send(codec.closing("SLOW_CONSUMER"));
            }
        } catch (Exception ignored) {
            // Best effort only; we are closing anyway.
        }
        close("SLOW_CONSUMER");
    }

    /** Idempotent close. Runs the registry cleanup callback exactly once. */
    public void close(String reason) {
        if (closed.compareAndSet(false, true)) {
            this.closeReason = reason;
            try {
                transport.close(reason);
            } catch (Exception ignored) {
                // Already gone.
            }
            try {
                onClose.accept(this);
            } catch (RuntimeException ex) {
                log.warn("connection cleanup failed for {}: {}", id(), ex.getClass().getSimpleName());
            }
        }
    }
}
