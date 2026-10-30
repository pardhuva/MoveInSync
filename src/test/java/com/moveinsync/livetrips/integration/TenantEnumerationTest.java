package com.moveinsync.livetrips.integration;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.error.ApiException;
import com.moveinsync.livetrips.gateway.ClientConnection;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripMeta;
import com.moveinsync.livetrips.service.PublishCommand;
import com.moveinsync.livetrips.support.GatewayHarness;
import com.moveinsync.livetrips.support.RecordingTransport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * Tenant-isolation hardening: a caller must not be able to use the response to tell whether a trip
 * id exists in <i>another</i> tenant.
 *
 * <p>Before this was fixed, the publish endpoint answered 403 FORBIDDEN for a trip that existed in
 * another tenant but 404 UNKNOWN_TRIP for one that did not exist at all &mdash; an existence oracle
 * that let any authenticated caller enumerate trip ids across tenant boundaries. Both cases must now
 * produce byte-identical responses.
 */
class TenantEnumerationTest {

    private GatewayHarness h;

    /** An authenticated acme device. Legitimate, but has no business knowing about globex. */
    private final AuthenticatedPrincipal acmeDevice =
            AuthenticatedPrincipal.device("dev-7", "acme", "cab-7");

    @BeforeEach
    void setUp() {
        h = new GatewayHarness();
        h.repository.registerOffice("office-BLR-1", "acme");
        h.repository.registerOffice("office-HYD-1", "globex");
        h.repository.save(new TripMeta("trip-100", "cab-7", "office-BLR-1", "acme", "emp-alice"));
        // Exists, but belongs to a different tenant.
        h.repository.save(new TripMeta("trip-200", "cab-99", "office-HYD-1", "globex", "emp-carol"));
    }

    @AfterEach
    void tearDown() {
        h.shutdown();
    }

    private ApiException publishExpectingFailure(String tripId) {
        return catchThrowableOfType(
                () -> h.publishService.publish(acmeDevice,
                        new PublishCommand(tripId, EventType.LOCATION, Map.of("lat", 1.0, "lng", 1.0))),
                ApiException.class);
    }

    @Test
    @DisplayName("PUBLISH: another tenant's existing trip is indistinguishable from a non-existent trip")
    void publishResponsesAreIndistinguishable() {
        ApiException crossTenant = publishExpectingFailure("trip-200");          // exists, globex
        ApiException nonExistent = publishExpectingFailure("trip-DOES-NOT-EXIST"); // exists nowhere

        assertThat(crossTenant).isNotNull();
        assertThat(nonExistent).isNotNull();

        // Every externally observable field must match: status, machine code and message.
        assertThat(crossTenant.status()).isEqualTo(nonExistent.status());
        assertThat(crossTenant.code()).isEqualTo(nonExistent.code());
        assertThat(crossTenant.getMessage()).isEqualTo(nonExistent.getMessage());

        // And specifically: no 403-vs-404 split that would reveal existence.
        assertThat(crossTenant.status().value()).isEqualTo(404);
        assertThat(crossTenant.code()).isEqualTo("UNKNOWN_TRIP");
    }

    @Test
    @DisplayName("PUBLISH: a same-tenant role denial still answers 403 (leaks nothing across tenants)")
    void sameTenantRoleDenialStillForbidden() {
        // cab-7's device publishing for a different cab *inside its own tenant*: the caller is
        // already inside this tenant, so a distinct 403 reveals nothing about another client.
        h.repository.save(new TripMeta("trip-101", "cab-8", "office-BLR-1", "acme", "emp-bob"));
        ApiException sameTenantOtherCab = publishExpectingFailure("trip-101");

        assertThat(sameTenantOtherCab).isNotNull();
        assertThat(sameTenantOtherCab.status().value()).isEqualTo(403);
        assertThat(sameTenantOtherCab.code()).isEqualTo("FORBIDDEN");
    }

    @Test
    @DisplayName("SUBSCRIBE: cross-tenant and unknown topics produce byte-identical error frames")
    void subscribeErrorFramesAreIndistinguishable() {
        AuthenticatedPrincipal acmeOperator = AuthenticatedPrincipal.operator("op-1", "acme");

        RecordingTransport t1 = new RecordingTransport("probe-cross-tenant");
        ClientConnection c1 = h.connect(t1, acmeOperator);
        h.subscriptionService.subscribe(c1, "trip:trip-200", 0); // exists, other tenant

        RecordingTransport t2 = new RecordingTransport("probe-nonexistent");
        ClientConnection c2 = h.connect(t2, acmeOperator);
        h.subscriptionService.subscribe(c2, "trip:trip-DOES-NOT-EXIST", 0); // exists nowhere

        List<String> crossTenantFrames = t1.frames();
        List<String> nonExistentFrames = t2.frames();

        assertThat(crossTenantFrames).hasSize(1);
        assertThat(nonExistentFrames).hasSize(1);
        // Identical JSON: same kind, same code, same message. Nothing to distinguish them.
        assertThat(crossTenantFrames.get(0)).isEqualTo(nonExistentFrames.get(0));
        assertThat(crossTenantFrames.get(0)).contains("\"code\":\"FORBIDDEN\"");

        // Neither frame echoes the probed trip id back to the caller.
        assertThat(crossTenantFrames.get(0)).doesNotContain("trip-200");
        assertThat(nonExistentFrames.get(0)).doesNotContain("trip-DOES-NOT-EXIST");
    }
}
