package com.moveinsync.livetrips.web;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.auth.JwtService;
import com.moveinsync.livetrips.error.Errors;
import com.moveinsync.livetrips.web.dto.DevTokenRequest;
import jakarta.validation.Valid;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.Set;

/**
 * Mints tokens for demos, tests and the load generator.
 *
 * <p><b>This is a deliberate hole and is disabled by default.</b> It exists only when
 * {@code livetrips.demo.token-endpoint-enabled=true}, which only the local {@code demo} profile
 * sets. With no profile active this endpoint is not registered at all and returns 404. A real deployment authenticates against
 * an identity provider and issues tokens there; handing out signed tokens for an arbitrary claimed
 * identity would defeat the entire authorisation model. It is included because the case study asks
 * for runnable sample API calls, and a reviewer needs a way to obtain a token.
 */
@RestController
@RequestMapping("/api/v1/dev")
@ConditionalOnProperty(name = "livetrips.demo.token-endpoint-enabled", havingValue = "true")
public class DevTokenController {

    private final JwtService jwtService;

    public DevTokenController(JwtService jwtService) {
        this.jwtService = jwtService;
    }

    @PostMapping("/token")
    public ResponseEntity<Map<String, Object>> issue(@Valid @RequestBody DevTokenRequest request) {
        AuthenticatedPrincipal principal = switch (request.role()) {
            case EMPLOYEE -> AuthenticatedPrincipal.employee(
                    request.userId(), request.tenantId(),
                    request.tripIds() == null ? Set.of() : request.tripIds());
            case OFFICE_ADMIN -> {
                if (request.officeId() == null) {
                    throw Errors.badRequest("officeId is required for OFFICE_ADMIN");
                }
                yield AuthenticatedPrincipal.officeAdmin(
                        request.userId(), request.tenantId(), request.officeId());
            }
            case OPERATOR -> AuthenticatedPrincipal.operator(request.userId(), request.tenantId());
            case DEVICE -> {
                if (request.cabId() == null) {
                    throw Errors.badRequest("cabId is required for DEVICE");
                }
                yield AuthenticatedPrincipal.device(
                        request.userId(), request.tenantId(), request.cabId());
            }
        };
        String token = jwtService.issue(principal);
        return ResponseEntity.ok(Map.of(
                "token", token,
                "role", request.role().name(),
                "userId", request.userId(),
                "tenantId", request.tenantId()));
    }
}
