package com.moveinsync.livetrips.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.NestedConfigurationProperty;

import java.time.Duration;

/**
 * Typed view of the {@code livetrips.*} configuration.
 *
 * <p>Every value here is a capacity or a bound that the case study asks to be stated explicitly,
 * so they are gathered in one place rather than scattered as {@code @Value} strings.
 */
@ConfigurationProperties(prefix = "livetrips")
public class LiveTripsProperties {

    /** Identifies this gateway node in logs, metrics and the welcome frame. */
    private String nodeId = "gw-local";

    @NestedConfigurationProperty
    private Auth auth = new Auth();
    @NestedConfigurationProperty
    private Replay replay = new Replay();
    @NestedConfigurationProperty
    private Gateway gateway = new Gateway();
    @NestedConfigurationProperty
    private Demo demo = new Demo();

    public String getNodeId() { return nodeId; }
    public void setNodeId(String nodeId) { this.nodeId = nodeId; }
    public Auth getAuth() { return auth; }
    public void setAuth(Auth auth) { this.auth = auth; }
    public Replay getReplay() { return replay; }
    public void setReplay(Replay replay) { this.replay = replay; }
    public Gateway getGateway() { return gateway; }
    public void setGateway(Gateway gateway) { this.gateway = gateway; }
    public Demo getDemo() { return demo; }
    public void setDemo(Demo demo) { this.demo = demo; }

    public static class Auth {
        /** HS256 signing key, supplied via LIVETRIPS_JWT_SECRET. Blank generates an ephemeral key. */
        private String jwtSecret = "";
        private Duration tokenTtl = Duration.ofHours(1);
        /** How long a socket may stay open without authenticating before it is closed. */
        private long handshakeTimeoutMs = 10_000;

        public String getJwtSecret() { return jwtSecret; }
        public void setJwtSecret(String jwtSecret) { this.jwtSecret = jwtSecret; }
        public Duration getTokenTtl() { return tokenTtl; }
        public void setTokenTtl(Duration tokenTtl) { this.tokenTtl = tokenTtl; }
        public long getHandshakeTimeoutMs() { return handshakeTimeoutMs; }
        public void setHandshakeTimeoutMs(long v) { this.handshakeTimeoutMs = v; }
    }

    public static class Replay {
        /**
         * Per-topic replay window in events. Miss more than this on a topic and you get a
         * snapshot instead of a replay. Memory: capacity x active topics x ~200 bytes.
         */
        private int capacity = 500;
        /** Redis mode: how long an idle topic's log and snapshot survive before reclamation. */
        private Duration topicTtl = Duration.ofHours(24);

        public int getCapacity() { return capacity; }
        public void setCapacity(int capacity) { this.capacity = capacity; }
        public Duration getTopicTtl() { return topicTtl; }
        public void setTopicTtl(Duration topicTtl) { this.topicTtl = topicTtl; }
    }

    public static class Gateway {
        /** Per-connection outbound queue bound, in events. */
        private int queueCapacity = 1000;
        /** Shared pool draining connection queues to sockets; sized for concurrent writers. */
        private int writerThreads = 32;
        private int maxTopicsPerConnection = 50;

        public int getQueueCapacity() { return queueCapacity; }
        public void setQueueCapacity(int v) { this.queueCapacity = v; }
        public int getWriterThreads() { return writerThreads; }
        public void setWriterThreads(int v) { this.writerThreads = v; }
        public int getMaxTopicsPerConnection() { return maxTopicsPerConnection; }
        public void setMaxTopicsPerConnection(int v) { this.maxTopicsPerConnection = v; }
    }

    public static class Demo {
        /** Seed demo tenants/offices/trips at startup so the service is immediately usable. */
        private boolean seedEnabled = true;

        public boolean isSeedEnabled() { return seedEnabled; }
        public void setSeedEnabled(boolean v) { this.seedEnabled = v; }
    }
}
