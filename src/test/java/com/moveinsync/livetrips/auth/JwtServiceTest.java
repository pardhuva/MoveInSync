package com.moveinsync.livetrips.auth;

import com.moveinsync.livetrips.error.ApiException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Authentication round-trips and the ways a token must be rejected. A token signed with one key
 * must not verify under another (the core of "nobody can forge an identity"), and an expired token
 * must be refused.
 */
class JwtServiceTest {

    private static final String SECRET = "test-secret-key-at-least-32-bytes-long-000";

    @Test
    @DisplayName("A token issued for a principal verifies back to the same identity and claims")
    void roundTripsIdentity() {
        JwtService jwt = new JwtService(SECRET, Duration.ofHours(1));
        AuthenticatedPrincipal alice = AuthenticatedPrincipal.employee("emp-alice", "acme", Set.of("trip-100"));

        AuthenticatedPrincipal back = jwt.verify(jwt.issue(alice));

        assertThat(back.userId()).isEqualTo("emp-alice");
        assertThat(back.tenantId()).isEqualTo("acme");
        assertThat(back.role()).isEqualTo(Role.EMPLOYEE);
        assertThat(back.ownsTrip("trip-100")).isTrue();
    }

    @Test
    @DisplayName("Role-specific claims survive the round trip")
    void roundTripsRoleSpecificClaims() {
        JwtService jwt = new JwtService(SECRET, Duration.ofHours(1));
        AuthenticatedPrincipal back = jwt.verify(jwt.issue(
                AuthenticatedPrincipal.officeAdmin("adm-1", "acme", "office-BLR-1")));
        assertThat(back.role()).isEqualTo(Role.OFFICE_ADMIN);
        assertThat(back.officeId()).isEqualTo("office-BLR-1");
    }

    @Test
    @DisplayName("A token signed with a different key is rejected (no forgery)")
    void rejectsForgedSignature() {
        JwtService issuer = new JwtService(SECRET, Duration.ofHours(1));
        JwtService other = new JwtService("a-totally-different-secret-key-32-bytes!!", Duration.ofHours(1));
        String token = issuer.issue(AuthenticatedPrincipal.operator("op-1", "acme"));

        assertThatThrownBy(() -> other.verify(token)).isInstanceOf(ApiException.class);
    }

    @Test
    @DisplayName("An expired token is rejected")
    void rejectsExpiredToken() {
        JwtService jwt = new JwtService(SECRET, Duration.ofHours(1));
        String token = jwt.issue(AuthenticatedPrincipal.operator("op-1", "acme"), Duration.ofSeconds(-1));
        assertThatThrownBy(() -> jwt.verify(token)).isInstanceOf(ApiException.class);
    }

    @Test
    @DisplayName("Garbage and null tokens are rejected, not crashed on")
    void rejectsGarbage() {
        JwtService jwt = new JwtService(SECRET, Duration.ofHours(1));
        assertThatThrownBy(() -> jwt.verify("not-a-jwt")).isInstanceOf(ApiException.class);
        assertThatThrownBy(() -> jwt.verify("")).isInstanceOf(ApiException.class);
    }

    @Test
    @DisplayName("bearerToken extracts the token only from a well-formed Bearer header")
    void bearerParsing() {
        assertThat(JwtService.bearerToken("Bearer abc.def.ghi")).isEqualTo("abc.def.ghi");
        assertThat(JwtService.bearerToken("abc.def.ghi")).isNull();
        assertThat(JwtService.bearerToken(null)).isNull();
    }
}
