package com.moveinsync.livetrips.model;

import java.util.Objects;

/**
 * Immutable ownership metadata for a trip. This is the authoritative data the
 * authorizer consults to decide who may subscribe. It answers: which cab, which
 * office, which tenant (client), and which employee owns this trip.
 *
 * <p>This is "hot data" &mdash; read on every subscribe &mdash; and is therefore cached
 * (see {@code TripDirectory}).
 */
public final class TripMeta {

    private final String tripId;
    private final String cabId;
    private final String officeId;
    private final String tenantId;
    private final String riderUserId;

    public TripMeta(String tripId, String cabId, String officeId, String tenantId, String riderUserId) {
        this.tripId = Objects.requireNonNull(tripId, "tripId");
        this.cabId = Objects.requireNonNull(cabId, "cabId");
        this.officeId = Objects.requireNonNull(officeId, "officeId");
        this.tenantId = Objects.requireNonNull(tenantId, "tenantId");
        this.riderUserId = Objects.requireNonNull(riderUserId, "riderUserId");
    }

    public String tripId() { return tripId; }
    public String cabId() { return cabId; }
    public String officeId() { return officeId; }
    public String tenantId() { return tenantId; }
    public String riderUserId() { return riderUserId; }

    @Override
    public String toString() {
        return "TripMeta{trip=" + tripId + ", cab=" + cabId + ", office=" + officeId
                + ", tenant=" + tenantId + "}";
    }
}
