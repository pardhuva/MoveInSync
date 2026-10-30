package com.moveinsync.livetrips.web.dto;

import com.moveinsync.livetrips.auth.Role;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.util.Set;

/**
 * Request for a demo token. See {@link com.moveinsync.livetrips.web.DevTokenController} for why
 * this endpoint must never be enabled outside a demo.
 */
public record DevTokenRequest(

        @NotBlank(message = "userId is required")
        String userId,

        @NotBlank(message = "tenantId is required")
        String tenantId,

        @NotNull(message = "role is required")
        Role role,

        /** Required for OFFICE_ADMIN. */
        String officeId,

        /** Required for DEVICE. */
        String cabId,

        /** For EMPLOYEE: the trips this user owns. */
        Set<String> tripIds
) {
}
