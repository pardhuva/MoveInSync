package com.moveinsync.livetrips.core;

/**
 * The result of an authorisation check on a subscribe attempt.
 *
 * <p>Carries two different messages on purpose: a precise {@link #reason()} for server-side
 * structured logs and metrics, and a deliberately vague {@link #clientMessage()} for the
 * client. Telling an attacker "that trip belongs to another tenant" confirms the trip exists;
 * the client is only ever told it is not permitted.
 */
public final class AuthorizationDecision {

    public enum DenyReason {
        /** Topic does not resolve to any known trip/cab/office. */
        UNKNOWN_TOPIC,
        /** The resource belongs to a different tenant (client). The tenant-isolation boundary. */
        TENANT_MISMATCH,
        /** Employee tried to watch a trip that is not theirs. */
        NOT_TRIP_OWNER,
        /** Office admin tried to watch an office other than their own. */
        NOT_OWN_OFFICE,
        /** This role may never subscribe to this kind of topic. */
        ROLE_NOT_PERMITTED,
        /** Topic string was syntactically invalid. */
        MALFORMED_TOPIC
    }

    private static final AuthorizationDecision ALLOWED = new AuthorizationDecision(true, null);

    private final boolean allowed;
    private final DenyReason reason;

    private AuthorizationDecision(boolean allowed, DenyReason reason) {
        this.allowed = allowed;
        this.reason = reason;
    }

    public static AuthorizationDecision allow() {
        return ALLOWED;
    }

    public static AuthorizationDecision deny(DenyReason reason) {
        return new AuthorizationDecision(false, reason);
    }

    public boolean isAllowed() { return allowed; }
    public boolean isDenied() { return !allowed; }

    /** Precise reason, for logs and metrics only. */
    public DenyReason reason() { return reason; }

    /** Safe, non-leaking message for the client. */
    public String clientMessage() {
        return allowed ? "ok" : "not permitted to subscribe to this topic";
    }

    @Override
    public String toString() {
        return allowed ? "ALLOW" : "DENY(" + reason + ")";
    }
}
