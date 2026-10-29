package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.model.Topic;
import com.moveinsync.livetrips.model.TripMeta;

import java.util.Objects;

/**
 * Decides whether a principal may subscribe to a topic. This is the security boundary the
 * case study calls out: <i>"Check this on subscribe, not just on login."</i>
 *
 * <p>Pure logic with one injected dependency ({@link TripMetadataSource}), so it is fully
 * unit-testable with no web server, Spring context or database.
 *
 * <h2>The rules</h2>
 * <ol>
 *   <li><b>Tenant isolation first.</b> Every topic resolves to an owning tenant. If that tenant
 *       is not the principal's tenant, the request is denied regardless of role. No role can
 *       cross a tenant boundary.</li>
 *   <li><b>EMPLOYEE</b> may subscribe to a {@code trip:} topic only if they own that trip, and to
 *       a {@code cab:} topic only if that cab's active trip is theirs. They may never subscribe
 *       to an {@code office:} topic.</li>
 *   <li><b>OFFICE_ADMIN</b> may subscribe to anything whose office equals their own office.</li>
 *   <li><b>OPERATOR</b> may subscribe to anything inside their own tenant, across offices.</li>
 * </ol>
 *
 * <p>Complexity: O(1) per check (one metadata lookup, served from a bounded cache).
 */
public class SubscriptionAuthorizer {

    private final TripMetadataSource metadata;

    public SubscriptionAuthorizer(TripMetadataSource metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    /** Authorise by raw topic string (as received from the client). */
    public AuthorizationDecision authorize(AuthenticatedPrincipal principal, String topicKey) {
        Topic topic;
        try {
            topic = Topic.parse(topicKey);
        } catch (RuntimeException ex) {
            return AuthorizationDecision.deny(AuthorizationDecision.DenyReason.MALFORMED_TOPIC);
        }
        return authorize(principal, topic);
    }

    public AuthorizationDecision authorize(AuthenticatedPrincipal principal, Topic topic) {
        Objects.requireNonNull(principal, "principal");
        Objects.requireNonNull(topic, "topic");

        return switch (topic.type()) {
            case TRIP -> authorizeTrip(principal, topic.id());
            case CAB -> authorizeCab(principal, topic.id());
            case OFFICE -> authorizeOffice(principal, topic.id());
        };
    }

    private AuthorizationDecision authorizeTrip(AuthenticatedPrincipal p, String tripId) {
        TripMeta meta = metadata.findTrip(tripId);
        if (meta == null) {
            return AuthorizationDecision.deny(AuthorizationDecision.DenyReason.UNKNOWN_TOPIC);
        }
        return decideForTrip(p, meta);
    }

    private AuthorizationDecision authorizeCab(AuthenticatedPrincipal p, String cabId) {
        TripMeta meta = metadata.findActiveTripForCab(cabId);
        if (meta == null) {
            // A cab is only watchable while it is on a trip; otherwise we cannot resolve its owner.
            return AuthorizationDecision.deny(AuthorizationDecision.DenyReason.UNKNOWN_TOPIC);
        }
        return decideForTrip(p, meta);
    }

    /** Shared rule set for trip- and cab-shaped topics, both of which resolve to a {@link TripMeta}. */
    private AuthorizationDecision decideForTrip(AuthenticatedPrincipal p, TripMeta meta) {
        // Rule 1: tenant isolation, checked before anything else.
        if (!meta.tenantId().equals(p.tenantId())) {
            return AuthorizationDecision.deny(AuthorizationDecision.DenyReason.TENANT_MISMATCH);
        }
        return switch (p.role()) {
            case EMPLOYEE -> {
                boolean mine = meta.riderUserId().equals(p.userId()) || p.ownsTrip(meta.tripId());
                yield mine
                        ? AuthorizationDecision.allow()
                        : AuthorizationDecision.deny(AuthorizationDecision.DenyReason.NOT_TRIP_OWNER);
            }
            case OFFICE_ADMIN -> meta.officeId().equals(p.officeId())
                    ? AuthorizationDecision.allow()
                    : AuthorizationDecision.deny(AuthorizationDecision.DenyReason.NOT_OWN_OFFICE);
            case OPERATOR -> AuthorizationDecision.allow(); // tenant already verified
            // A cab device is a producer, never a consumer.
            case DEVICE -> AuthorizationDecision.deny(AuthorizationDecision.DenyReason.ROLE_NOT_PERMITTED);
        };
    }

    private AuthorizationDecision authorizeOffice(AuthenticatedPrincipal p, String officeId) {
        String tenant = metadata.findOfficeTenant(officeId);
        if (tenant == null) {
            return AuthorizationDecision.deny(AuthorizationDecision.DenyReason.UNKNOWN_TOPIC);
        }
        if (!tenant.equals(p.tenantId())) {
            return AuthorizationDecision.deny(AuthorizationDecision.DenyReason.TENANT_MISMATCH);
        }
        return switch (p.role()) {
            // An employee watches their own trip, never a whole office.
            case EMPLOYEE -> AuthorizationDecision.deny(AuthorizationDecision.DenyReason.ROLE_NOT_PERMITTED);
            case OFFICE_ADMIN -> officeId.equals(p.officeId())
                    ? AuthorizationDecision.allow()
                    : AuthorizationDecision.deny(AuthorizationDecision.DenyReason.NOT_OWN_OFFICE);
            case OPERATOR -> AuthorizationDecision.allow();
            case DEVICE -> AuthorizationDecision.deny(AuthorizationDecision.DenyReason.ROLE_NOT_PERMITTED);
        };
    }
}
