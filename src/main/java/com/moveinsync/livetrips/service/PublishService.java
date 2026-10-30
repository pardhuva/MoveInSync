package com.moveinsync.livetrips.service;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.core.AuthorizationDecision;
import com.moveinsync.livetrips.core.EventLog;
import com.moveinsync.livetrips.core.PublishAuthorizer;
import com.moveinsync.livetrips.core.SnapshotStore;
import com.moveinsync.livetrips.distribution.OrderedDispatcher;
import com.moveinsync.livetrips.error.Errors;
import com.moveinsync.livetrips.metrics.LiveTripsMetrics;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.Topic;
import com.moveinsync.livetrips.model.TopicType;
import com.moveinsync.livetrips.model.TripEvent;
import com.moveinsync.livetrips.model.TripMeta;
import com.moveinsync.livetrips.repository.TripRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * The write path: authorise, sequence, persist to the bounded log, update the snapshot, distribute.
 *
 * <h2>The order of operations matters</h2>
 * <ol>
 *   <li><b>Authorise</b> against the trip record. The caller supplies only a trip id and a payload;
 *       cab, office and tenant are read from the trip record, never trusted from the request. A
 *       device cannot relabel its event as belonging to another cab or office.</li>
 *   <li><b>Sequence and append atomically</b> ({@link EventLog#append}). This is the single point
 *       where order is decided, so concurrent publishers on one trip can never produce a
 *       mis-ordered log.</li>
 *   <li><b>Record the snapshot</b> before distributing, so a client that resumes immediately after
 *       cannot see a snapshot that is older than an event it already received.</li>
 *   <li><b>Distribute</b> to all interested gateway nodes.</li>
 * </ol>
 *
 * <h2>Fan-out to three topics, and what it costs</h2>
 * One real-world update becomes up to three log entries (trip, cab, office), each with its own
 * sequence, because the case study makes all three independently subscribable and sequences are
 * per topic. The cost is a 3&times; write amplification on the publish path. For an office topic
 * covering a large site this is the most expensive stream in the system, which is why
 * {@code livetrips.publish.office-topics-enabled} exists &mdash; see the trade-offs section of the
 * README.
 *
 * <h2>Latency semantics</h2>
 * The origin timestamp is stamped <i>here</i>, at ingest, not taken from the request. Trusting a
 * device clock would make the delivery-latency metric meaningless under clock skew. The metric
 * therefore measures <b>ingest &rarr; socket write</b>. True end-to-end latency (including both
 * network hops) is measured separately and client-side by the load harness.
 */
@Service
public class PublishService {

    private static final Logger log = LoggerFactory.getLogger(PublishService.class);

    private final EventLog eventLog;
    private final SnapshotStore snapshotStore;
    private final OrderedDispatcher dispatcher;
    private final TripRepository repository;
    private final PublishAuthorizer publishAuthorizer;
    private final LiveTripsMetrics metrics;
    private final boolean officeTopicsEnabled;

    public PublishService(EventLog eventLog,
                          SnapshotStore snapshotStore,
                          OrderedDispatcher dispatcher,
                          TripRepository repository,
                          PublishAuthorizer publishAuthorizer,
                          LiveTripsMetrics metrics,
                          @Value("${livetrips.publish.office-topics-enabled:true}") boolean officeTopicsEnabled) {
        this.eventLog = eventLog;
        this.snapshotStore = snapshotStore;
        this.dispatcher = dispatcher;
        this.repository = repository;
        this.publishAuthorizer = publishAuthorizer;
        this.metrics = metrics;
        this.officeTopicsEnabled = officeTopicsEnabled;
    }

    /**
     * Publish one update.
     *
     * @return the sequence assigned on each topic the event was written to
     */
    public Map<String, Long> publish(AuthenticatedPrincipal principal, PublishCommand command) {
        AuthorizationDecision decision = publishAuthorizer.authorize(principal, command.tripId());
        if (decision.isDenied()) {
            metrics.publishRejected();
            // Structured, PII-free: who (opaque id), what was refused, why. No payload.
            log.warn("publish_denied user={} tenant={} role={} trip={} reason={}",
                    principal.userId(), principal.tenantId(), principal.role(),
                    command.tripId(), decision.reason());
            // A trip in ANOTHER tenant must be indistinguishable from a trip that does not exist:
            // returning 403 for one and 404 for the other would let any authenticated caller probe
            // trip ids and learn which exist in other tenants. Both therefore answer "no such trip".
            // ROLE_NOT_PERMITTED and friends still answer 403, which leaks nothing across tenants:
            // reaching that branch already means the caller is inside the resource's own tenant.
            throw switch (decision.reason()) {
                case UNKNOWN_TOPIC, TENANT_MISMATCH -> Errors.unknownTrip();
                default -> Errors.forbidden();
            };
        }

        TripMeta meta = repository.findTrip(command.tripId());
        if (meta == null) {
            // Raced with the trip ending between the authorisation check and now.
            metrics.publishRejected();
            throw Errors.unknownTrip();
        }

        long ingestTimestamp = System.currentTimeMillis();
        String eventId = UUID.randomUUID().toString();

        // A template builder shared across topics. Topic is stamped per iteration below; each
        // build() produces a distinct immutable event. (A topic is mandatory on a built event, so
        // the template is never built without one.)
        TripEvent.Builder base = TripEvent.builder()
                .eventId(eventId)
                .type(command.type())
                .tripId(meta.tripId())
                .cabId(meta.cabId())
                .officeId(meta.officeId())
                .payload(command.payload())
                .originTimestampMs(ingestTimestamp);

        boolean isSos = command.type() == EventType.SOS;
        Map<String, Long> sequences = new LinkedHashMap<>();
        for (Topic topic : topicsFor(meta, command.type())) {
            final Topic t = topic;
            TripEvent sequenced;
            if (isSos) {
                // SOS bypasses the ordered partitions so it skips ahead of queued traffic at the
                // dispatch stage too. Its true position is preserved by its sequence number (the log
                // is still appended in order) and by the per-connection express lane.
                sequenced = eventLog.append(base.topic(t).build());
                snapshotStore.record(sequenced);
                dispatcher.dispatchImmediate(sequenced);
            } else {
                // Sequence assignment and enqueue are atomic per topic, so live distribution happens
                // in strict sequence order. The fan-out itself runs off-lock on a partition thread.
                sequenced = dispatcher.dispatchOrdered(t.key(), () -> eventLog.append(base.topic(t).build()));
                snapshotStore.record(sequenced);
            }
            sequences.put(t.key(), sequenced.sequence());
        }

        metrics.published(command.type());
        if (command.type() != EventType.LOCATION) {
            // Status and SOS are rare and operationally interesting: always worth a log line.
            // The payload is omitted because a location or an SOS note can identify a person.
            log.info("event_published type={} trip={} cab={} office={} sequences={}",
                    command.type(), meta.tripId(), meta.cabId(), meta.officeId(), sequences);
        }

        maybeEndTrip(command, meta);
        return sequences;
    }

    /** The topics one update is written to. */
    private List<Topic> topicsFor(TripMeta meta, EventType type) {
        List<Topic> topics = new ArrayList<>(3);
        topics.add(Topic.of(TopicType.TRIP, meta.tripId()));
        topics.add(Topic.of(TopicType.CAB, meta.cabId()));
        if (officeTopicsEnabled) {
            topics.add(Topic.of(TopicType.OFFICE, meta.officeId()));
        }
        return topics;
    }

    /**
     * A terminal status releases the cab so it stops resolving to this trip. Topic state is
     * deliberately <i>not</i> deleted here: a client that was offline when the trip ended must
     * still be able to resume and see the final status. State is reclaimed by TTL (Redis mode) or
     * by an explicit admin call.
     */
    private void maybeEndTrip(PublishCommand command, TripMeta meta) {
        if (command.type() != EventType.STATUS) {
            return;
        }
        Object status = command.payload().get("status");
        if (status == null) {
            return;
        }
        String s = status.toString().toUpperCase();
        if (s.equals("COMPLETED") || s.equals("CANCELLED")) {
            repository.endTrip(meta.tripId());
            log.info("trip_ended trip={} cab={} status={}", meta.tripId(), meta.cabId(), s);
        }
    }
}
