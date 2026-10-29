package com.moveinsync.livetrips;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for the Live Trip Updates fan-out service.
 *
 * <p>This service is the publish-subscribe layer that delivers live cab location,
 * trip status, delay and SOS updates to security desks, transport admins and
 * employees over WebSocket. See {@code docs/} for the architecture and the
 * mapping of case-study requirements to code.
 */
@SpringBootApplication
public class LiveTripUpdatesApplication {

    public static void main(String[] args) {
        SpringApplication.run(LiveTripUpdatesApplication.class, args);
    }
}
