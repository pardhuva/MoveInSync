package com.moveinsync.livetrips.service;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.core.AuthorizationDecision;
import com.moveinsync.livetrips.core.CriticalHistory;
import com.moveinsync.livetrips.core.EventLog;
import com.moveinsync.livetrips.core.ReplayResult;
import com.moveinsync.livetrips.core.SnapshotStore;
import com.moveinsync.livetrips.core.SubscriptionAuthorizer;
import com.moveinsync.livetrips.distribution.EventDistributor;
import com.moveinsync.livetrips.gateway.ClientConnection;
import com.moveinsync.livetrips.gateway.ConnectionRegistry;
import com.moveinsync.livetrips.gateway.FrameCodec;
import com.moveinsync.livetrips.metrics.LiveTripsMetrics;
import com.moveinsync.livetrips.model.TopicSnapshot;
import com.moveinsync.livetrips.model.TripEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Set;

/**
 * The read path: authorise a subscribe, then bring the client to a correct starting point.
 *
 * <h2>The three ways a subscribe can start</h2>
 * <ul>
 *   <li><b>LIVE</b> &mdash; the client's {@code lastSeq} already equals the topic's latest sequence.
 *       Nothing to send; live events follow.</li>
 *   <li><b>REPLAY</b> &mdash; the client was away briefly and everything it missed is still inside
 *       the bounded window. It receives exactly the missed events, in sequence order. This is the
 *       case-study example: a phone in a lift for 40 seconds gets the three status changes it
 *       missed, in order.</li>
 *   <li><b>SNAPSHOT</b> &mdash; the client was away too long (its next needed event has been evicted),
 *       or it is subscribing for the first time. It receives one snapshot of current state stamped
 *       with a sequence, then live events. Intermediate history inside the gap is deliberately not
 *       reconstructed; that is the documented cost of a bounded buffer.</li>
 * </ul>
 *
 * <h2>Why the subscription is registered before the replay is read</h2>
 * Registering first means an event published during setup lands in the client's queue <i>and</i>
 * may also appear in the replay &mdash; a duplicate, which client-side dedup removes. Reading the
 * replay first would instead leave a window in which an event belongs to neither the replay nor the
 * live stream: a silent gap. Duplicates are recoverable, gaps are not, so the order is deliberate.
 * The whole setup runs under the connection's write lock so no live event can overtake the replay.
 */
@Service
public class SubscriptionService {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionService.class);

    private final ConnectionRegistry registry;
    private final SubscriptionAuthorizer authorizer;
    private final EventLog eventLog;
    private final SnapshotStore snapshotStore;
    private final EventDistributor distributor;
    private final FrameCodec codec;
    private final LiveTripsMetrics metrics;
    private final int maxTopicsPerConnection;

    public SubscriptionService(ConnectionRegistry registry,
                               SubscriptionAuthorizer authorizer,
                               EventLog eventLog,
                               SnapshotStore snapshotStore,
                               EventDistributor distributor,
                               FrameCodec codec,
                               LiveTripsMetrics metrics,
                               @Value("${livetrips.gateway.max-topics-per-connection:50}") int maxTopicsPerConnection) {
        this.registry = registry;
        this.authorizer = authorizer;
        this.eventLog = eventLog;
        this.snapshotStore = snapshotStore;
        this.distributor = distributor;
        this.codec = codec;
        this.metrics = metrics;
        this.maxTopicsPerConnection = maxTopicsPerConnection;
    }

    /** The starting mode chosen for a subscribe. */
    public enum StartMode { LIVE, REPLAY, SNAPSHOT }

    /**
     * Authorise and set up a subscription, emitting the ack and any replay/snapshot frames.
     *
     * @param lastSeq the highest sequence the client has already applied for this topic; 0 for a
     *                fresh subscribe
     * @return the mode the subscription started in, or null if it was refused
     */
    public StartMode subscribe(ClientConnection connection, String topicKey, long lastSeq) {
        AuthenticatedPrincipal principal = connection.principal();
        if (principal == null) {
            connection.sendNow(codec.error("UNAUTHENTICATED", "authenticate before subscribing"));
            return null;
        }

        AuthorizationDecision decision = authorizer.authorize(principal, topicKey);
        if (decision.isDenied()) {
            metrics.subscribeDenied(decision.reason());
            // This is a security-relevant event: record who, what and why, with opaque ids only.
            log.warn("subscribe_denied user={} tenant={} role={} topic={} reason={}",
                    principal.userId(), principal.tenantId(), principal.role(), topicKey,
                    decision.reason());
            connection.sendNow(codec.error("FORBIDDEN", decision.clientMessage()));
            return null;
        }

        if (!connection.topics().contains(topicKey)
                && connection.topics().size() >= maxTopicsPerConnection) {
            // A bound on per-connection memory: each topic adds queue and bookkeeping cost.
            connection.sendNow(codec.error("TOO_MANY_SUBSCRIPTIONS",
                    "subscription limit reached for this connection"));
            return null;
        }

        metrics.subscribeAllowed();

        connection.lockWrites();
        try {
            // 1. Register first: live events start queueing (they cannot be written while we hold
            //    the write lock), so nothing published from here on can be missed.
            if (registry.subscribe(connection, topicKey)) {
                distributor.onLocalSubscribe(topicKey);
            }

            // 2. Decide the starting point.
            if (lastSeq <= 0) {
                return startFromSnapshot(connection, topicKey, lastSeq, StartMode.SNAPSHOT);
            }
            ReplayResult result = eventLog.replayFrom(topicKey, lastSeq);
            if (result.isSnapshotRequired()) {
                return startFromSnapshot(connection, topicKey, lastSeq, StartMode.SNAPSHOT);
            }
            if (result.events().isEmpty()) {
                connection.sendNow(codec.subscribed(topicKey, StartMode.LIVE.name(),
                        result.latestSequence()));
                metrics.resume(StartMode.LIVE.name());
                return StartMode.LIVE;
            }
            connection.sendNow(codec.subscribed(topicKey, StartMode.REPLAY.name(),
                    result.latestSequence()));
            for (TripEvent event : result.events()) {
                connection.sendReplayEventNow(event);
            }
            metrics.resume(StartMode.REPLAY.name());
            log.debug("subscribe_replay user={} topic={} from={} count={}",
                    principal.userId(), topicKey, lastSeq, result.events().size());
            return StartMode.REPLAY;
        } finally {
            connection.unlockWrites();
        }
    }

    /**
     * Snapshot-path start: replay every STATUS/SOS the client missed (from the bounded critical
     * history, independent of the evicted location window), then send the latest-state snapshot.
     *
     * <p>Order matters: the missed critical events are sent <b>before</b> the snapshot frame,
     * because a client treats the snapshot as "you are now current at atSeq" and would otherwise
     * drop those criticals as duplicates. If the critical history itself had overflowed for this
     * client's horizon, {@code truncated} is set so the client knows some intermediate criticals
     * are unrecoverable and only the snapshot's latest state is authoritative for them.
     */
    private StartMode startFromSnapshot(ClientConnection connection, String topicKey,
                                        long lastSeq, StartMode mode) {
        TopicSnapshot snapshot = snapshotStore.snapshot(topicKey);
        CriticalHistory critical = snapshotStore.criticalEventsAfter(topicKey, lastSeq);

        connection.sendNow(codec.subscribed(topicKey, mode.name(), snapshot.atSequence(),
                critical.truncated()));
        for (TripEvent event : critical.events()) {
            connection.sendReplayEventNow(event);
        }
        connection.sendNow(codec.snapshot(snapshot));

        metrics.resume(mode.name());
        if (critical.truncated()) {
            metrics.criticalHistoryTruncated();
            log.warn("critical_history_truncated topic={} fromSeq={}: some STATUS/SOS older than "
                    + "the critical-history horizon could not be replayed", topicKey, lastSeq);
        }
        return mode;
    }

    /** Remove one subscription and drop the node's interest if it was the last local subscriber. */
    public void unsubscribe(ClientConnection connection, String topicKey) {
        if (registry.unsubscribe(connection, topicKey)) {
            distributor.onLocalUnsubscribe(topicKey);
        }
        connection.sendNow(codec.unsubscribed(topicKey));
    }

    /**
     * Full teardown when a connection closes for any reason (clean close, network drop, or this
     * node being killed). Every topic that loses its last local subscriber is dropped from the
     * backbone, so a dead node stops receiving traffic it cannot deliver.
     */
    public void disconnect(ClientConnection connection) {
        Set<String> noLongerInterested = registry.unregister(connection);
        for (String topic : noLongerInterested) {
            distributor.onLocalUnsubscribe(topic);
        }
        metrics.connectionClosed(connection.closeReason());
        log.debug("connection_closed id={} reason={} topics_released={}",
                connection.id(), connection.closeReason(), noLongerInterested.size());
    }

    /** Current replay window size, surfaced on the health endpoint and used by docs/tests. */
    public int replayCapacity() {
        return eventLog.capacity();
    }
}
