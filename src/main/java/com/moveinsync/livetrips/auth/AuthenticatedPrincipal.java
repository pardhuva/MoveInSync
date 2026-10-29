package com.moveinsync.livetrips.auth;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * The authenticated identity extracted from a verified JWT. This is the subject whose access is
 * checked on every subscribe and every publish.
 *
 * <p>Deliberately carries no PII (no name, phone, email or home location) &mdash; only the opaque
 * ids needed for authorisation, so it is safe to include in logs.
 */
public final class AuthenticatedPrincipal {

    private final String userId;
    private final String tenantId;
    private final Role role;
    /** Set for OFFICE_ADMIN: the office they administer. Null otherwise. */
    private final String officeId;
    /** Set for DEVICE: the only cab this principal may publish for. Null otherwise. */
    private final String cabId;
    /** For EMPLOYEE: trip ids they personally own. Empty for other roles. */
    private final Set<String> ownedTripIds;

    public AuthenticatedPrincipal(String userId, String tenantId, Role role,
                                  String officeId, String cabId, Set<String> ownedTripIds) {
        this.userId = Objects.requireNonNull(userId, "userId");
        this.tenantId = Objects.requireNonNull(tenantId, "tenantId");
        this.role = Objects.requireNonNull(role, "role");
        this.officeId = officeId;
        this.cabId = cabId;
        this.ownedTripIds = ownedTripIds == null ? Collections.emptySet() : Set.copyOf(ownedTripIds);
    }

    public static AuthenticatedPrincipal employee(String userId, String tenantId, Set<String> ownedTripIds) {
        return new AuthenticatedPrincipal(userId, tenantId, Role.EMPLOYEE, null, null, ownedTripIds);
    }

    public static AuthenticatedPrincipal officeAdmin(String userId, String tenantId, String officeId) {
        return new AuthenticatedPrincipal(userId, tenantId, Role.OFFICE_ADMIN, officeId, null, null);
    }

    public static AuthenticatedPrincipal operator(String userId, String tenantId) {
        return new AuthenticatedPrincipal(userId, tenantId, Role.OPERATOR, null, null, null);
    }

    public static AuthenticatedPrincipal device(String deviceId, String tenantId, String cabId) {
        return new AuthenticatedPrincipal(deviceId, tenantId, Role.DEVICE, null, cabId, null);
    }

    public String userId() { return userId; }
    public String tenantId() { return tenantId; }
    public Role role() { return role; }
    public String officeId() { return officeId; }
    public String cabId() { return cabId; }
    public Set<String> ownedTripIds() { return ownedTripIds; }

    public boolean ownsTrip(String tripId) {
        return ownedTripIds.contains(tripId);
    }

    @Override
    public String toString() {
        // PII-safe: opaque ids and role only.
        return "Principal{user=" + userId + ", tenant=" + tenantId + ", role=" + role
                + ", office=" + officeId + ", cab=" + cabId + "}";
    }
}
