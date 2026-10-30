package com.moveinsync.livetrips.core;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.core.AuthorizationDecision.DenyReason;
import com.moveinsync.livetrips.model.TripMeta;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Per-subscription authorization and, above all, tenant isolation.
 *
 * <p>The case study calls out one test by name: "a test that tries to subscribe to another
 * client's trip". {@link #employeeCannotSubscribeToAnotherTenantsTrip()} and
 * {@link #operatorCannotCrossTenantBoundary()} are that test, from two angles.
 */
class SubscriptionAuthorizerTest {

    /** A hand-built metadata source so the authorizer tests touch no Spring, cache or repository. */
    private static final class FakeMetadata implements TripMetadataSource {
        final Map<String, TripMeta> trips = new HashMap<>();
        final Map<String, String> officeTenant = new HashMap<>();

        FakeMetadata with(TripMeta m) {
            trips.put(m.tripId(), m);
            officeTenant.put(m.officeId(), m.tenantId());
            return this;
        }

        public TripMeta findTrip(String tripId) { return trips.get(tripId); }
        public TripMeta findActiveTripForCab(String cabId) {
            return trips.values().stream().filter(t -> t.cabId().equals(cabId)).findFirst().orElse(null);
        }
        public String findOfficeTenant(String officeId) { return officeTenant.get(officeId); }
    }

    // acme owns trip-100 (cab-7, office-BLR-1, rider alice); globex owns trip-200.
    private final FakeMetadata metadata = new FakeMetadata()
            .with(new TripMeta("trip-100", "cab-7", "office-BLR-1", "acme", "emp-alice"))
            .with(new TripMeta("trip-200", "cab-99", "office-HYD-1", "globex", "emp-carol"));

    private final SubscriptionAuthorizer authorizer = new SubscriptionAuthorizer(metadata);

    @Test
    @DisplayName("TENANT ISOLATION: an employee cannot subscribe to another client's trip")
    void employeeCannotSubscribeToAnotherTenantsTrip() {
        // Alice is a legitimate acme employee, even owns a trip, but trip-200 belongs to globex.
        AuthenticatedPrincipal alice = AuthenticatedPrincipal.employee("emp-alice", "acme", Set.of("trip-200"));

        AuthorizationDecision decision = authorizer.authorize(alice, "trip:trip-200");

        assertThat(decision.isDenied()).isTrue();
        // Denied on the tenant boundary, before any role/ownership rule -- and note it is denied
        // even though her (forged) token claims to own trip-200. Ownership cannot cross tenants.
        assertThat(decision.reason()).isEqualTo(DenyReason.TENANT_MISMATCH);
    }

    @Test
    @DisplayName("TENANT ISOLATION: an operator cannot cross into another tenant via any topic")
    void operatorCannotCrossTenantBoundary() {
        AuthenticatedPrincipal acmeOperator = AuthenticatedPrincipal.operator("op-1", "acme");

        assertThat(authorizer.authorize(acmeOperator, "trip:trip-200").reason()).isEqualTo(DenyReason.TENANT_MISMATCH);
        assertThat(authorizer.authorize(acmeOperator, "cab:cab-99").reason()).isEqualTo(DenyReason.TENANT_MISMATCH);
        assertThat(authorizer.authorize(acmeOperator, "office:office-HYD-1").reason()).isEqualTo(DenyReason.TENANT_MISMATCH);
    }

    @Test
    @DisplayName("An employee may subscribe to their own trip and its cab")
    void employeeCanWatchOwnTrip() {
        AuthenticatedPrincipal alice = AuthenticatedPrincipal.employee("emp-alice", "acme", Set.of("trip-100"));
        assertThat(authorizer.authorize(alice, "trip:trip-100").isAllowed()).isTrue();
        assertThat(authorizer.authorize(alice, "cab:cab-7").isAllowed()).isTrue();
    }

    @Test
    @DisplayName("An employee may not subscribe to a same-tenant trip that is not theirs")
    void employeeCannotWatchColleaguesTrip() {
        // bob is in acme but trip-100 is alice's.
        AuthenticatedPrincipal bob = AuthenticatedPrincipal.employee("emp-bob", "acme", Set.of());
        AuthorizationDecision d = authorizer.authorize(bob, "trip:trip-100");
        assertThat(d.isDenied()).isTrue();
        assertThat(d.reason()).isEqualTo(DenyReason.NOT_TRIP_OWNER);
    }

    @Test
    @DisplayName("An employee may not subscribe to a whole office")
    void employeeCannotWatchOffice() {
        AuthenticatedPrincipal alice = AuthenticatedPrincipal.employee("emp-alice", "acme", Set.of("trip-100"));
        assertThat(authorizer.authorize(alice, "office:office-BLR-1").reason()).isEqualTo(DenyReason.ROLE_NOT_PERMITTED);
    }

    @Test
    @DisplayName("An office admin may watch their own office but not another office in the same tenant")
    void officeAdminScopedToOwnOffice() {
        // Add a second acme office so we test same-tenant, different-office.
        metadata.with(new TripMeta("trip-101", "cab-8", "office-BLR-2", "acme", "emp-dan"));
        AuthenticatedPrincipal blr1Admin = AuthenticatedPrincipal.officeAdmin("adm-1", "acme", "office-BLR-1");

        assertThat(authorizer.authorize(blr1Admin, "office:office-BLR-1").isAllowed()).isTrue();
        assertThat(authorizer.authorize(blr1Admin, "office:office-BLR-2").reason()).isEqualTo(DenyReason.NOT_OWN_OFFICE);
        assertThat(authorizer.authorize(blr1Admin, "trip:trip-101").reason()).isEqualTo(DenyReason.NOT_OWN_OFFICE);
    }

    @Test
    @DisplayName("A cab device may never subscribe to anything")
    void deviceCannotSubscribe() {
        AuthenticatedPrincipal device = AuthenticatedPrincipal.device("dev-7", "acme", "cab-7");
        assertThat(authorizer.authorize(device, "trip:trip-100").reason()).isEqualTo(DenyReason.ROLE_NOT_PERMITTED);
    }

    @Test
    @DisplayName("Unknown and malformed topics are denied without leaking existence")
    void unknownAndMalformed() {
        AuthenticatedPrincipal op = AuthenticatedPrincipal.operator("op-1", "acme");
        assertThat(authorizer.authorize(op, "trip:does-not-exist").reason()).isEqualTo(DenyReason.UNKNOWN_TOPIC);
        assertThat(authorizer.authorize(op, "garbage-no-colon").reason()).isEqualTo(DenyReason.MALFORMED_TOPIC);
    }
}
