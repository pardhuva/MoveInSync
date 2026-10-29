package com.moveinsync.livetrips.model;

/**
 * The three kinds of topic a client can subscribe to, as required by the case study
 * ("Topics -- per cab, per trip and per office").
 *
 * <p>Each topic is an independent ordered stream with its own per-topic sequence
 * number and its own bounded replay buffer. A single real-world update is published
 * to every topic it belongs to (e.g. a location for cab C on trip T in office O is
 * written to {@code cab:C}, {@code trip:T} and {@code office:O}), each assigning its
 * own sequence. Clients dedup and resume per topic.
 */
public enum TopicType {

    CAB("cab"),
    TRIP("trip"),
    OFFICE("office");

    private final String code;

    TopicType(String code) {
        this.code = code;
    }

    public String code() {
        return code;
    }

    public static TopicType fromCode(String code) {
        for (TopicType t : values()) {
            if (t.code.equalsIgnoreCase(code)) {
                return t;
            }
        }
        throw new IllegalArgumentException("Unknown topic type: " + code);
    }
}
