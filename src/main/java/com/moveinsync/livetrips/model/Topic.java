package com.moveinsync.livetrips.model;

import java.util.Objects;

/**
 * A subscribable channel identified by a {@link TopicType} and an id.
 *
 * <p>The canonical string form is {@code "<type>:<id>"}, e.g. {@code "trip:T-100"}.
 * This string is the key used everywhere: as the sequencer key, the replay-buffer
 * key, the Redis channel name and the client-facing subscription key.
 */
public final class Topic {

    private final TopicType type;
    private final String id;
    private final String key;

    public Topic(TopicType type, String id) {
        this.type = Objects.requireNonNull(type, "type");
        this.id = Objects.requireNonNull(id, "id");
        if (id.isBlank()) {
            throw new IllegalArgumentException("topic id must not be blank");
        }
        this.key = type.code() + ":" + id;
    }

    public static Topic of(TopicType type, String id) {
        return new Topic(type, id);
    }

    /** Parse the canonical {@code "<type>:<id>"} form. */
    public static Topic parse(String key) {
        Objects.requireNonNull(key, "key");
        int idx = key.indexOf(':');
        if (idx <= 0 || idx == key.length() - 1) {
            throw new IllegalArgumentException("Invalid topic key: " + key);
        }
        return new Topic(TopicType.fromCode(key.substring(0, idx)), key.substring(idx + 1));
    }

    public TopicType type() {
        return type;
    }

    public String id() {
        return id;
    }

    /** Canonical {@code "<type>:<id>"} string. */
    public String key() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Topic other)) return false;
        return key.equals(other.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    @Override
    public String toString() {
        return key;
    }
}
