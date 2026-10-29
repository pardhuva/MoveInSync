package com.moveinsync.livetrips.auth;

import com.moveinsync.livetrips.error.Errors;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Issues and verifies the HS256 JWTs that authenticate every connection and publish.
 *
 * <h2>Claims</h2>
 * {@code sub} (user or device id), {@code tenant}, {@code role}, and optionally {@code office},
 * {@code cab} and {@code trips}. Only opaque identifiers &mdash; never a name, phone number or
 * email &mdash; so a token that ends up in a log file leaks nothing personal.
 *
 * <h2>Why the signing key is not in the repository</h2>
 * The key is read from configuration ({@code livetrips.auth.jwt-secret}, normally supplied as the
 * {@code LIVETRIPS_JWT_SECRET} environment variable). If it is absent the service generates a
 * random key at startup and logs a warning: that keeps the repository free of credentials and makes
 * a misconfigured deployment obvious rather than silently insecure. A generated key means tokens do
 * not survive a restart and are not valid on other nodes, so any real multi-node run must set the
 * variable.
 *
 * <h2>Scope note</h2>
 * Token <i>issuance</i> ({@link #issue}) lives here so the demo and the load generator can mint
 * tokens. In production the identity provider would own issuance and this service would only
 * verify.
 */
public class JwtService {

    private static final Logger log = LoggerFactory.getLogger(JwtService.class);

    private static final String CLAIM_TENANT = "tenant";
    private static final String CLAIM_ROLE = "role";
    private static final String CLAIM_OFFICE = "office";
    private static final String CLAIM_CAB = "cab";
    private static final String CLAIM_TRIPS = "trips";

    private final SecretKey key;
    private final Duration defaultTtl;
    private final boolean usingGeneratedKey;

    public JwtService(String secret, Duration defaultTtl) {
        this.defaultTtl = defaultTtl;
        if (secret == null || secret.isBlank()) {
            byte[] random = new byte[32];
            new SecureRandom().nextBytes(random);
            this.key = Keys.hmacShaKeyFor(random);
            this.usingGeneratedKey = true;
            log.warn("No livetrips.auth.jwt-secret configured: generated an ephemeral signing key. "
                    + "Tokens will not survive a restart and will not be accepted by other nodes. "
                    + "Set the LIVETRIPS_JWT_SECRET environment variable for any real deployment.");
        } else {
            byte[] bytes = decodeSecret(secret);
            if (bytes.length < 32) {
                throw new IllegalStateException(
                        "livetrips.auth.jwt-secret must provide at least 256 bits (32 bytes) for HS256");
            }
            this.key = Keys.hmacShaKeyFor(bytes);
            this.usingGeneratedKey = false;
        }
    }

    /** Accepts either a base64 secret or a raw passphrase of sufficient length. */
    private static byte[] decodeSecret(String secret) {
        try {
            return Base64.getDecoder().decode(secret);
        } catch (IllegalArgumentException notBase64) {
            return secret.getBytes(StandardCharsets.UTF_8);
        }
    }

    public boolean isUsingGeneratedKey() {
        return usingGeneratedKey;
    }

    public String issue(AuthenticatedPrincipal principal) {
        return issue(principal, defaultTtl);
    }

    public String issue(AuthenticatedPrincipal principal, Duration ttl) {
        Instant now = Instant.now();
        var builder = Jwts.builder()
                .subject(principal.userId())
                .claim(CLAIM_TENANT, principal.tenantId())
                .claim(CLAIM_ROLE, principal.role().name())
                .issuedAt(Date.from(now))
                .expiration(Date.from(now.plus(ttl)));
        if (principal.officeId() != null) {
            builder.claim(CLAIM_OFFICE, principal.officeId());
        }
        if (principal.cabId() != null) {
            builder.claim(CLAIM_CAB, principal.cabId());
        }
        if (!principal.ownedTripIds().isEmpty()) {
            builder.claim(CLAIM_TRIPS, List.copyOf(principal.ownedTripIds()));
        }
        return builder.signWith(key).compact();
    }

    /**
     * Verify a token's signature and expiry and map it to a principal.
     *
     * @throws com.moveinsync.livetrips.error.ApiException with code INVALID_TOKEN for any failure.
     *         The specific reason (bad signature vs expired vs malformed) is deliberately not
     *         distinguished to the caller.
     */
    @SuppressWarnings("unchecked")
    public AuthenticatedPrincipal verify(String token) {
        if (token == null || token.isBlank()) {
            throw Errors.unauthenticated();
        }
        Claims claims;
        try {
            claims = Jwts.parser().verifyWith(key).build().parseSignedClaims(token).getPayload();
        } catch (Exception ex) {
            // Log the class only: a rejected token string must not be written to disk.
            log.debug("token rejected: {}", ex.getClass().getSimpleName());
            throw Errors.invalidToken();
        }
        try {
            String userId = claims.getSubject();
            String tenant = claims.get(CLAIM_TENANT, String.class);
            Role role = Role.valueOf(claims.get(CLAIM_ROLE, String.class));
            String office = claims.get(CLAIM_OFFICE, String.class);
            String cab = claims.get(CLAIM_CAB, String.class);
            Object tripsClaim = claims.get(CLAIM_TRIPS);
            Set<String> trips = tripsClaim instanceof List<?> list
                    ? Set.copyOf(((List<Object>) list).stream().map(String::valueOf).toList())
                    : Set.of();
            if (userId == null || tenant == null) {
                throw Errors.invalidToken();
            }
            return new AuthenticatedPrincipal(userId, tenant, role, office, cab, trips);
        } catch (IllegalArgumentException | NullPointerException ex) {
            throw Errors.invalidToken();
        }
    }

    /** Extract a bearer token from an Authorization header value, or null. */
    public static String bearerToken(String authorizationHeader) {
        if (authorizationHeader == null) {
            return null;
        }
        String prefix = "Bearer ";
        return authorizationHeader.regionMatches(true, 0, prefix, 0, prefix.length())
                ? authorizationHeader.substring(prefix.length()).trim()
                : null;
    }
}
