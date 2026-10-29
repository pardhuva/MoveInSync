package com.moveinsync.livetrips.config;

import com.moveinsync.livetrips.gateway.LiveTripWebSocketHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

/**
 * Registers the subscriber endpoint at {@code /ws/live}.
 *
 * <h2>Why raw WebSocket and not STOMP</h2>
 * Spring's STOMP support ships a broker-relay and its own subscription model, which would take
 * over exactly the parts this case study is about: the per-connection bounded queue, location
 * conflation, the SOS priority lane and resume-from-sequence. Implementing the protocol directly
 * keeps those decisions in our own code where they can be tested and defended.
 *
 * <h2>Buffer limits are a denial-of-service control</h2>
 * Client frames are small (a subscribe is a few hundred bytes), so the inbound limit is set low:
 * a client cannot make the server buffer megabytes on its behalf. The send buffer and send time
 * limit are a second line of defence behind our own {@link com.moveinsync.livetrips.core.OutboundQueue}
 * &mdash; if a socket write blocks because the peer has stopped reading, the container abandons it
 * rather than pinning a writer thread indefinitely.
 */
@Configuration
@EnableWebSocket
public class WebSocketConfig implements WebSocketConfigurer {

    private final LiveTripWebSocketHandler handler;

    public WebSocketConfig(LiveTripWebSocketHandler handler) {
        this.handler = handler;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        registry.addHandler(handler, "/ws/live")
                // Browsers from any origin may connect; the token, not the origin, is the
                // security boundary. Tighten this to the known dashboard origins in production.
                .setAllowedOriginPatterns("*");
    }

    @Bean
    public ServletServerContainerFactoryBean webSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(8 * 1024);
        container.setMaxBinaryMessageBufferSize(8 * 1024);
        container.setMaxSessionIdleTimeout(5 * 60_000L);
        // Give up on a blocked write rather than letting it hold a writer thread forever.
        container.setAsyncSendTimeout(5_000L);
        return container;
    }
}
