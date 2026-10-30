package com.moveinsync.livetrips.web.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

/**
 * Registers a trip's ownership so the authorizer can answer "who may watch this?".
 *
 * <p>The tenant is deliberately absent: it is taken from the caller's own token, so an operator
 * physically cannot create a trip inside another client's tenant, even by crafting the request.
 */
public record RegisterTripRequest(

        @NotBlank(message = "tripId is required")
        @Size(max = 64, message = "tripId must be at most 64 characters")
        String tripId,

        @NotBlank(message = "cabId is required")
        @Size(max = 64, message = "cabId must be at most 64 characters")
        String cabId,

        @NotBlank(message = "officeId is required")
        @Size(max = 64, message = "officeId must be at most 64 characters")
        String officeId,

        /** The employee who owns this trip; only they may subscribe to it as an EMPLOYEE. */
        @NotBlank(message = "riderUserId is required")
        @Size(max = 64, message = "riderUserId must be at most 64 characters")
        String riderUserId
) {
}
