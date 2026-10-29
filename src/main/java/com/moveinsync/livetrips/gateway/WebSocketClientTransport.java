package com.moveinsync.livetrips.gateway;

import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;

import java.io.IOException;

/**
 * {@link ClientTransport} over a Spring {@link WebSocketSession}.
 *
 * <p>Thread-safety is provided by the caller, not here: {@link ClientConnection} serialises every
 * write behind its own lock, which is what a {@code WebSocketSession} requires (concurrent
 * {@code sendMessage} calls on one session corrupt the frame stream).
 */
public class WebSocketClientTransport implements ClientTransport {

    private final WebSocketSession session;

    public WebSocketClientTransport(WebSocketSession session) {
        this.session = session;
    }

    @Override
    public String id() {
        return session.getId();
    }

    @Override
    public void send(String payload) throws IOException {
        session.sendMessage(new TextMessage(payload));
    }

    @Override
    public void close(String reason) {
        try {
            if (session.isOpen()) {
                // Close reasons are capped at 123 bytes by the WebSocket spec.
                String safe = reason == null ? "closed" : reason;
                if (safe.length() > 100) {
                    safe = safe.substring(0, 100);
                }
                session.close(CloseStatus.NORMAL.withReason(safe));
            }
        } catch (IOException | IllegalStateException ignored) {
            // Already closed or closing; nothing useful to do.
        }
    }

    @Override
    public boolean isOpen() {
        return session.isOpen();
    }
}
