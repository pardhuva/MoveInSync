package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.model.TripMeta;

import java.util.Objects;

/**
 * Decides who may <i>produce</i> an update for a trip. The read side
 * ({@link SubscriptionAuthorizer}) and the write side are separate classes because they answer
 * different questions and have different threat models: reading another tenant's stream is a
 * privacy breach, while writing into it is data forgery (a fake SOS, or a spoofed "trip completed").
 *
 * <h2>The rules</h2>
 * <ol>
 *   <li>Tenant isolation first: the trip must belong to the principal's tenant.</li>
 *   <li><b>DEVICE</b> may publish only for the cab named in its own token, and only while that cab
 *       is the one assigned to the trip. A stolen in-vehicle device therefore cannot fabricate
 *       events for any other cab.</li>
 *   <li><b>OPERATOR</b> may publish for any trip in their tenant (this is the telemetry-ingest and
 *       control-room path).</li>
 *   <li><b>EMPLOYEE</b> and <b>OFFICE_ADMIN</b> may not publish at all. Note this means raising an
 *       SOS from a rider's phone is not modelled here; see the limitations section of the README.</li>
 * </ol>
 */
public class PublishAuthorizer {

    private final TripMetadataSource metadata;

    public PublishAuthorizer(TripMetadataSource metadata) {
        this.metadata = Objects.requireNonNull(metadata, "metadata");
    }

    public AuthorizationDecision authorize(AuthenticatedPrincipal principal, String tripId) {
        Objects.requireNonNull(principal, "principal");

        TripMeta meta = metadata.findTrip(tripId);
        if (meta == null) {
            return AuthorizationDecision.deny(AuthorizationDecision.DenyReason.UNKNOWN_TOPIC);
        }
        if (!meta.tenantId().equals(principal.tenantId())) {
            return AuthorizationDecision.deny(AuthorizationDecision.DenyReason.TENANT_MISMATCH);
        }
        return switch (principal.role()) {
            case DEVICE -> meta.cabId().equals(principal.cabId())
                    ? AuthorizationDecision.allow()
                    : AuthorizationDecision.deny(AuthorizationDecision.DenyReason.ROLE_NOT_PERMITTED);
            case OPERATOR -> AuthorizationDecision.allow();
            case EMPLOYEE, OFFICE_ADMIN ->
                    AuthorizationDecision.deny(AuthorizationDecision.DenyReason.ROLE_NOT_PERMITTED);
        };
    }
}
