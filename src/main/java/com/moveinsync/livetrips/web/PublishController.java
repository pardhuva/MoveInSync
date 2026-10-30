package com.moveinsync.livetrips.web;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.service.PublishCommand;
import com.moveinsync.livetrips.service.PublishService;
import com.moveinsync.livetrips.web.dto.PublishEventRequest;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Where cabs push updates. A cab is a producer, not a browser, so it uses a plain authenticated
 * HTTP POST rather than a WebSocket: producers are fire-and-forget and need none of the
 * subscription machinery.
 *
 * <p>The returned map is {@code topicKey -> assignedSequence}, so a producer (or a test) can see
 * exactly which per-topic sequence its update became. Authorisation, trip resolution and
 * sequencing all happen inside {@link PublishService}; this class only validates the shape of the
 * request and names the authenticated caller.
 */
@RestController
@RequestMapping("/api/v1/trips")
public class PublishController {

    private final PublishService publishService;

    public PublishController(PublishService publishService) {
        this.publishService = publishService;
    }

    @PostMapping("/{tripId}/events")
    public ResponseEntity<Map<String, Long>> publish(
            AuthenticatedPrincipal principal,
            @PathVariable String tripId,
            @Valid @RequestBody PublishEventRequest request) {

        request.validateForType();
        PublishCommand command = new PublishCommand(tripId, request.type(), request.toPayload());
        Map<String, Long> assignedSequences = publishService.publish(principal, command);
        return ResponseEntity.accepted().body(assignedSequences);
    }
}
