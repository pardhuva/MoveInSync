package com.moveinsync.livetrips.auth;

/**
 * Role-based access levels. Authorisation is enforced per subscription (see
 * {@link com.moveinsync.livetrips.core.SubscriptionAuthorizer}), not merely at login.
 */
public enum Role {

    /** An employee/rider. May watch only trips they personally own. */
    EMPLOYEE,

    /** An office admin / security desk. May watch anything within their own office. */
    OFFICE_ADMIN,

    /** A transport operator. May watch anything within their own tenant (client), across offices. */
    OPERATOR,

    /**
     * A cab's telemetry device. Publishes updates for its own cab only and may not subscribe to
     * anything. Separating this from the human roles means a compromised in-vehicle device can
     * emit data but can never read another trip's stream.
     */
    DEVICE
}
