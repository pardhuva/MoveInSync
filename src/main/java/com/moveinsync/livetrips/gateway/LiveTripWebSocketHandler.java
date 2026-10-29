package com.moveinsync.livetrips.gateway;

import com.moveinsync.livetrips.auth.AuthenticatedPrincipal;
import com.moveinsync.livetrips.auth.JwtService;
import com.moveinsync.livetrips.config.LiveTripsProperties;
import com.moveinsync.livetrips.core.OutboundQueue;
import com.moveinsync.livetrips.error.ApiException;
import com.moveinsync.livetrips.metrics.LiveTripsMetrics;
import com.moveinsync.livetrips.service.DeliveryService;
import com.moveinsync.livetrips.service.SubscriptionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The WebSocket endpoint subscribers connect to.
 *
 * <h2>Authentication: why no token in the URL</h2>
 * A credential in a query string gets written to access logs, proxy logs and browser history. So
 * this handler accepts a token one of two ways: an {@code Authorization: Bearer} handshake header
 * (what server-side clients and the load generator use), or an {@code authenticate} frame sent as
 * the first message (what a browser uses, since browsers cannot set handshake headers on a
 * WebSocket). A socket that has not authenticated within
 * {@code livetrips.auth.handshake-timeout-ms} is closed, so an unauthenticated connection cannot
 * sit and consume a slot.
 *
 * <h2>What this class does and does not do</h2>
 * It parses frames and delegates. Every decision that matters &mdash; authorisation, resume mode,
 * back-pressure &mdash; lives in the service and core layers, which is why they can be tested
 * without a servlet container.
 */
@Component
public class LiveTripWebSocketHandler extends TextWebSocketHandler {

    private static final Logger log = LoggerFactory.getLogger(LiveTripWebSocketHandler.class);
    private static final String ATTR_CONNECTION = "ltConnection";

    private final JwtService jwtService;
    private final SubscriptionService subscriptionService;
    private final ConnectionRegistry registry;
    private final FrameCodec codec;
    private final LiveTripsMetrics metrics;
    private final DeliveryService deliveryService;
    private final ThreadPoolExecutor writerPool;
    private final LiveTripsProperties props;

    /** Single small scheduler for handshake timeouts; daemon so it never holds up shutdown. */
    private final ScheduledExecutorService timeouts = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "ws-handshake-timeout");
        t.setDaemon(true);
        return t;
    });

    public LiveTripWebSocketHandler(JwtService jwtService,
                                    SubscriptionService subscriptionService,
                                    ConnectionRegistry registry,
                                    FrameCodec codec,
                                    LiveTripsMetrics metrics,
                                    DeliveryService deliveryService,
                                    ThreadPoolExecutor writerPool,
                                    LiveTripsProperties props) {
        this.jwtService = jwtService;
        this.subscriptionService = subscriptionService;
        this.registry = registry;
        this.codec = codec;
        this.metrics = metrics;
        this.deliveryService = deliveryService;
        this.writerPool = writerPool;
        this.props = props;
    }

    @Override
    public void afterConnectionEstablished(WebSocketSession session) {
        ClientConnection connection = new ClientConnection(
                new WebSocketClientTransport(session),
                new OutboundQueue(props.getGateway().getQueueCapacity()),
                codec,
                writerPool,
                deliveryService);

        connection.onClose(subscriptionService::disconnect);
        session.getAttributes().put(ATTR_CONNECTION, connection);
        registry.register(connection);
        metrics.connectionOpened();

        // Preferred path: token supplied as a handshake header.
        String headerToken = JwtService.bearerToken(
                session.getHandshakeHeaders().getFirst("Authorization"));
        if (headerToken != null) {
            tryAuthenticate(connection, headerToken);
        }

        connection.sendNow(codec.welcome(props.getNodeId()));
        scheduleHandshakeTimeout(connection);
    }

    private void scheduleHandshakeTimeout(ClientConnection connection) {
        long timeout = props.getAuth().getHandshakeTimeoutMs();
        if (timeout <= 0) {
            return;
        }
        timeouts.schedule(() -> {
            if (connection.isOpen() && !connection.isAuthenticated()) {
                connection.sendNow(codec.error("UNAUTHENTICATED", "authentication timed out"));
                connection.close("AUTH_TIMEOUT");
            }
        }, timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    protected void handleTextMessage(WebSocketSession session, TextMessage message) {
        ClientConnection connection = (ClientConnection) session.getAttributes().get(ATTR_CONNECTION);
        if (connection == null) {
            return;
        }
        Map<String, Object> frame;
        try {
            frame = codec.decodeClientFrame(message.getPayload());
        } catch (RuntimeException ex) {
            connection.sendNow(codec.error("BAD_FRAME", "frame is not valid JSON"));
            return;
        }

        Object action = frame.get("action");
        if (action == null) {
            connection.sendNow(codec.error("BAD_FRAME", "missing action"));
            return;
        }

        try {
            switch (action.toString()) {
                case "authenticate" -> handleAuthenticate(connection, frame);
                case "subscribe" -> handleSubscribe(connection, frame);
                case "unsubscribe" -> handleUnsubscribe(connection, frame);
                case "ping" -> connection.sendNow(codec.pong());
                default -> connection.sendNow(codec.error("BAD_FRAME", "unknown action"));
            }
        } catch (ApiException ex) {
            connection.sendNow(codec.error(ex.code(), ex.getMessage()));
        } catch (RuntimeException ex) {
            // Never surface internals to the client; log the type only.
            log.warn("frame handling failed on connection {}: {}",
                    connection.id(), ex.getClass().getSimpleName());
            connection.sendNow(codec.error("INTERNAL_ERROR", "the request could not be completed"));
        }
    }

    private void handleAuthenticate(ClientConnection connection, Map<String, Object> frame) {
        Object token = frame.get("token");
        if (token == null) {
            connection.sendNow(codec.error("UNAUTHENTICATED", "token is required"));
            return;
        }
        tryAuthenticate(connection, token.toString());
    }

    private void tryAuthenticate(ClientConnection connection, String token) {
        try {
            AuthenticatedPrincipal principal = jwtService.verify(token);
            connection.authenticate(principal);
            connection.sendNow(codec.authenticated(principal.userId(), principal.role().name()));
            log.debug("connection_authenticated id={} user={} tenant={} role={}",
                    connection.id(), principal.userId(), principal.tenantId(), principal.role());
        } catch (ApiException ex) {
            metrics.authFailure();
            connection.sendNow(codec.error(ex.code(), ex.getMessage()));
            // An invalid token is not retried: close rather than let it probe.
            connection.close("AUTH_FAILED");
        }
    }

    private void handleSubscribe(ClientConnection connection, Map<String, Object> frame) {
        Object topic = frame.get("topic");
        if (topic == null) {
            connection.sendNow(codec.error("BAD_FRAME", "topic is required"));
            return;
        }
        long lastSeq = asLong(frame.get("lastSeq"));
        subscriptionService.subscribe(connection, topic.toString(), lastSeq);
    }

    private void handleUnsubscribe(ClientConnection connection, Map<String, Object> frame) {
        Object topic = frame.get("topic");
        if (topic == null) {
            connection.sendNow(codec.error("BAD_FRAME", "topic is required"));
            return;
        }
        subscriptionService.unsubscribe(connection, topic.toString());
    }

    private static long asLong(Object o) {
        if (o == null) {
            return 0L;
        }
        if (o instanceof Number n) {
            return n.longValue();
        }
        try {
            return Long.parseLong(o.toString());
        } catch (NumberFormatException ex) {
            return 0L;
        }
    }

    @Override
    public void afterConnectionClosed(WebSocketSession session, CloseStatus status) {
        ClientConnection connection = (ClientConnection) session.getAttributes().remove(ATTR_CONNECTION);
        if (connection != null) {
            // close() is idempotent and runs the registry/backbone cleanup exactly once.
            connection.close(connection.closeReason() != null
                    ? connection.closeReason()
                    : "CLIENT_CLOSED_" + status.getCode());
        }
    }

    @Override
    public void handleTransportError(WebSocketSession session, Throwable exception) {
        ClientConnection connection = (ClientConnection) session.getAttributes().get(ATTR_CONNECTION);
        if (connection != null) {
            log.debug("transport_error id={} type={}", connection.id(),
                    exception.getClass().getSimpleName());
            connection.close("TRANSPORT_ERROR");
        }
    }
}
