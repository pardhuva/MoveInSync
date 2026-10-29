package com.moveinsync.livetrips.error;

import jakarta.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Turns every exception into a small, safe JSON body and makes sure the detail lands in the log
 * instead of the response.
 *
 * <h2>The correlation id</h2>
 * Each failure gets a short random {@code errorId}, returned to the caller and logged alongside the
 * real cause. A user can quote it in a ticket and an engineer can find the stack trace, without the
 * response body ever containing internals.
 *
 * <h2>PII discipline</h2>
 * Request bodies are never logged (a location update is personal data: it says where a named
 * employee is right now). Only the method, path, error code and exception type are recorded.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(ApiException.class)
    public ResponseEntity<Map<String, Object>> handleApi(ApiException ex, HttpServletRequest request) {
        String errorId = newErrorId();
        // Expected, handled conditions: one line at INFO, no stack trace.
        log.info("request_failed errorId={} code={} status={} method={} path={}",
                errorId, ex.code(), ex.status().value(), request.getMethod(), request.getRequestURI());
        return ResponseEntity.status(ex.status()).body(body(ex.code(), ex.getMessage(), errorId));
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex,
                                                                HttpServletRequest request) {
        String errorId = newErrorId();
        // Field names are safe to echo; field *values* are not, so only names are included.
        String fields = ex.getBindingResult().getFieldErrors().stream()
                .map(e -> e.getField() + ": " + e.getDefaultMessage())
                .reduce((a, b) -> a + "; " + b)
                .orElse("invalid request");
        log.info("request_invalid errorId={} method={} path={}",
                errorId, request.getMethod(), request.getRequestURI());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(body("BAD_REQUEST", fields, errorId));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgument(IllegalArgumentException ex,
                                                                     HttpServletRequest request) {
        String errorId = newErrorId();
        log.info("request_invalid errorId={} method={} path={} type={}",
                errorId, request.getMethod(), request.getRequestURI(), ex.getClass().getSimpleName());
        return ResponseEntity.status(HttpStatus.BAD_REQUEST)
                .body(body("BAD_REQUEST", "invalid request", errorId));
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleUnexpected(Exception ex, HttpServletRequest request) {
        String errorId = newErrorId();
        // Unexpected: this one gets the stack trace, server-side only.
        log.error("request_error errorId={} method={} path={}",
                errorId, request.getMethod(), request.getRequestURI(), ex);
        ApiException internal = Errors.internal();
        return ResponseEntity.status(internal.status())
                .body(body(internal.code(), internal.getMessage(), errorId));
    }

    private static Map<String, Object> body(String code, String message, String errorId) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("code", code);
        m.put("message", message);
        m.put("errorId", errorId);
        return m;
    }

    private static String newErrorId() {
        return UUID.randomUUID().toString().substring(0, 8);
    }
}
