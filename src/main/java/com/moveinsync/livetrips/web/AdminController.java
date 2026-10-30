package com.moveinsync.livetrips.web;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.auth.Role;
import com.moveinsync.livetrips.error.Errors;
import com.moveinsync.livetrips.model.TripMeta;
import com.moveinsync.livetrips.repository.TripRepository;
import com.moveinsync.livetrips.service.CachingTripMetadataService;
import com.moveinsync.livetrips.web.dto.RegisterTripRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Trip registration. In a real deployment this would be driven by the dispatch system; it is
 * exposed here so the demo and tests can set up ownership.
 *
 * <p>Tenant isolation is enforced on write as well as read: the trip is created inside the
 * caller's own tenant, taken from the token, never from the body. Only OPERATOR and OFFICE_ADMIN
 * may register trips; an OFFICE_ADMIN may only register into their own office.
 */
@RestController
@RequestMapping("/api/v1/admin")
public class AdminController {

    private final TripRepository tripRepository;
    private final CachingTripMetadataService cache;

    public AdminController(TripRepository tripRepository, CachingTripMetadataService cache) {
        this.tripRepository = tripRepository;
        this.cache = cache;
    }

    @PostMapping("/trips")
    public ResponseEntity<TripMeta> registerTrip(AuthenticatedPrincipal principal,
                                                 @Valid @RequestBody RegisterTripRequest request) {
        if (principal.role() == Role.EMPLOYEE || principal.role() == Role.DEVICE) {
            throw Errors.forbidden();
        }
        if (principal.role() == Role.OFFICE_ADMIN
                && !request.officeId().equals(principal.officeId())) {
            // An office admin cannot plant a trip in a different office.
            throw Errors.forbidden();
        }

        TripMeta meta = new TripMeta(
                request.tripId(), request.cabId(), request.officeId(),
                principal.tenantId(), request.riderUserId());
        tripRepository.registerOffice(request.officeId(), principal.tenantId());
        tripRepository.save(meta);
        // Ownership just changed; drop any cached view so the new record is authoritative at once.
        cache.invalidateTrip(request.tripId());
        return ResponseEntity.ok(meta);
    }

    @PostMapping("/trips/{tripId}/end")
    public ResponseEntity<Map<String, String>> endTrip(AuthenticatedPrincipal principal,
                                                       @org.springframework.web.bind.annotation.PathVariable String tripId) {
        if (principal.role() == Role.EMPLOYEE || principal.role() == Role.DEVICE) {
            throw Errors.forbidden();
        }
        TripMeta meta = tripRepository.findTrip(tripId);
        if (meta == null || !meta.tenantId().equals(principal.tenantId())) {
            // Don't reveal existence of trips in other tenants.
            throw Errors.unknownTrip();
        }
        tripRepository.endTrip(tripId);
        cache.invalidateTrip(tripId);
        return ResponseEntity.ok(Map.of("tripId", tripId, "status", "ended"));
    }
}
