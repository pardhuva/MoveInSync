package com.moveinsync.livetrips.model;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * An immutable update as it exists <b>within one topic</b>.
 *
 * <p>The same real-world update (identified by {@link #eventId()}) is published to
 * every topic it belongs to; each topic produces its own {@code TripEvent} with its
 * own {@link #sequence()}. Sequence numbers are therefore meaningful only within a
 * single topic &mdash; that is exactly the "per-topic sequence number" the case study
 * requires for ordering, dedup and resume.
 *
 * <p>Instances are created through {@link Builder} and are safe to share across threads.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public final class TripEvent {

    /** Shared across every per-topic copy of the same real-world update. Enables cross-topic dedup. */
    private final String eventId;
    /** The topic key this copy belongs to, e.g. {@code "trip:T-1"}. */
    private final String topic;
    /** Per-topic monotonically increasing sequence number, assigned by the sequencer. 0 before assignment. */
    private final long sequence;
    private final EventType type;

    private final String tripId;
    private final String cabId;
    private final String officeId;

    /** Arbitrary non-PII payload (e.g. lat/lng for LOCATION, status code for STATUS). */
    private final Map<String, Object> payload;

    /** Epoch millis when the update originated at the producer (used to measure delivery latency). */
    private final long originTimestampMs;

    private TripEvent(Builder b) {
        this.eventId = Objects.requireNonNull(b.eventId, "eventId");
        this.topic = Objects.requireNonNull(b.topic, "topic");
        this.sequence = b.sequence;
        this.type = Objects.requireNonNull(b.type, "type");
        this.tripId = b.tripId;
        this.cabId = b.cabId;
        this.officeId = b.officeId;
        this.payload = b.payload == null
                ? Collections.emptyMap()
                : Collections.unmodifiableMap(new HashMap<>(b.payload));
        this.originTimestampMs = b.originTimestampMs;
    }

    public String eventId() { return eventId; }
    public String topic() { return topic; }
    public long sequence() { return sequence; }
    public EventType type() { return type; }
    public String tripId() { return tripId; }
    public String cabId() { return cabId; }
    public String officeId() { return officeId; }
    public Map<String, Object> payload() { return payload; }
    public long originTimestampMs() { return originTimestampMs; }

    /**
     * The key used to conflate (collapse) backlogged updates of the same kind.
     * Only LOCATION conflates, keyed by cab so a client that lags keeps the latest
     * position of each cab. Non-conflatable types return {@code null}.
     */
    public String conflationKey() {
        return type.isConflatable() ? "loc:" + cabId : null;
    }

    /** Returns a copy with the sequence assigned (events are sequenced after construction). */
    public TripEvent withSequence(long seq) {
        return toBuilder().sequence(seq).build();
    }

    public Builder toBuilder() {
        return new Builder()
                .eventId(eventId).topic(topic).sequence(sequence).type(type)
                .tripId(tripId).cabId(cabId).officeId(officeId)
                .payload(payload).originTimestampMs(originTimestampMs);
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "TripEvent{topic=" + topic + ", seq=" + sequence + ", type=" + type
                + ", cab=" + cabId + ", trip=" + tripId + "}";
    }

    public static final class Builder {
        private String eventId;
        private String topic;
        private long sequence;
        private EventType type;
        private String tripId;
        private String cabId;
        private String officeId;
        private Map<String, Object> payload;
        private long originTimestampMs;

        public Builder eventId(String v) { this.eventId = v; return this; }
        public Builder topic(String v) { this.topic = v; return this; }
        public Builder topic(Topic v) { this.topic = v.key(); return this; }
        public Builder sequence(long v) { this.sequence = v; return this; }
        public Builder type(EventType v) { this.type = v; return this; }
        public Builder tripId(String v) { this.tripId = v; return this; }
        public Builder cabId(String v) { this.cabId = v; return this; }
        public Builder officeId(String v) { this.officeId = v; return this; }
        public Builder payload(Map<String, Object> v) { this.payload = v; return this; }
        public Builder originTimestampMs(long v) { this.originTimestampMs = v; return this; }

        public TripEvent build() {
            return new TripEvent(this);
        }
    }
}
