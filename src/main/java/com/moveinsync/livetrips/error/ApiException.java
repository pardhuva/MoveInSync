package com.moveinsync.livetrips.error;

import org.springframework.http.HttpStatus;

/**
 * Base class for errors this service reports deliberately.
 *
 * <p>Carries three things on purpose:
 * <ul>
 *   <li>a stable machine-readable {@link #code()} clients can branch on;</li>
 *   <li>a {@link #getMessage() message} that is safe to return over the wire &mdash; no stack traces,
 *       no internal class names, no user PII, no indication of whether a resource that the caller
 *       is not allowed to see actually exists;</li>
 *   <li>an HTTP {@link #status()}.</li>
 * </ul>
 *
 * <p>Anything more detailed belongs in the server log, keyed by a correlation id, never in the
 * response body.
 */
public class ApiException extends RuntimeException {

    private final String code;
    private final HttpStatus status;

    public ApiException(String code, HttpStatus status, String safeMessage) {
        super(safeMessage);
        this.code = code;
        this.status = status;
    }

    public String code() {
        return code;
    }

    public HttpStatus status() {
        return status;
    }
}
