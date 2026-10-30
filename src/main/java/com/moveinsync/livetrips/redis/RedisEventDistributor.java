package com.moveinsync.livetrips.redis;

import com.moveinsync.livetrips.codec.TripEventCodec;
import com.moveinsync.livetrips.distribution.EventDistributor;
import com.moveinsync.livetrips.model.EventType;
import com.moveinsync.livetrips.model.TripEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.RedisMessageListenerContainer;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Multi-node {@link EventDistributor} over Redis Pub/Sub.
 *
 * <h2>How an event reaches the node holding a subscriber</h2>
 * Each topic maps to a Redis channel. A node issues {@code SUBSCRIBE} for a topic's channel when
 * its first local client subscribes, and {@code UNSUBSCRIBE} when its last one leaves. Redis then
 * delivers each published message only to the nodes currently subscribed. <b>Redis's own
 * subscription registry is the routing table</b> &mdash; there is no separate "node holds client"
 * directory for us to keep consistent, and no node needs to know about any other node.
 *
 * <h2>The SOS priority lane, at the distribution hop</h2>
 * SOS events travel on a <i>separate channel prefix</i> served by a <i>separate listener
 * container</i>, which means a separate Redis connection and a separate dispatch thread pool. A
 * burst of location messages therefore cannot queue ahead of an SOS at this hop: the two have
 * independent sockets and independent consumer threads. This is the distribution-layer half of
 * "SOS skips ahead everywhere in the pipeline"; the per-connection half is
 * {@link com.moveinsync.livetrips.core.OutboundQueue}'s express lane.
 *
 * <h2>Known cost (documented trade-off)</h2>
 * Subscribe/unsubscribe churn is proportional to topic churn, and Redis Pub/Sub is fire-and-forget:
 * a message published while a node is briefly disconnected is lost <i>for the live path</i>. That
 * loss is not a correctness problem because the durable record is {@link RedisEventLog}: the client
 * notices the sequence gap and resumes. Pub/Sub is the fast path, the log is the truth.
 */
public class RedisEventDistributor implements EventDistributor {

    private static final Logger log = LoggerFactory.getLogger(RedisEventDistributor.class);

    private static final String CHANNEL_NORMAL = "lt:ch:";
    private static final String CHANNEL_SOS = "lt:sos:";

    private final StringRedisTemplate redis;
    private final RedisMessageListenerContainer normalContainer;
    private final RedisMessageListenerContainer sosContainer;
    private final TripEventCodec codec;
    private final String nodeId;

    private final Set<String> subscribedTopics = ConcurrentHashMap.newKeySet();
    private volatile EventListener listener;

    /** One listener instance reused for every channel; the event itself carries its topic. */
    private final MessageListener messageListener;

    public RedisEventDistributor(StringRedisTemplate redis,
                                 RedisMessageListenerContainer normalContainer,
                                 RedisMessageListenerContainer sosContainer,
                                 TripEventCodec codec,
                                 String nodeId) {
        this.redis = redis;
        this.normalContainer = normalContainer;
        this.sosContainer = sosContainer;
        this.codec = codec;
        this.nodeId = nodeId;
        // Built here, not as a field initializer, so it captures the already-assigned codec.
        this.messageListener = (message, pattern) -> {
            EventListener l = listener;
            if (l == null) {
                return;
            }
            try {
                TripEvent event = codec.decode(new String(message.getBody(), StandardCharsets.UTF_8));
                l.onEvent(event);
            } catch (RuntimeException ex) {
                // A malformed message must never kill the listener thread: log and drop.
                log.warn("Dropping undecodable event from backbone: {}", ex.getMessage());
            }
        };
    }

    @Override
    public void start(EventListener listener) {
        this.listener = listener;
    }

    @Override
    public void publish(TripEvent event) {
        String channel = (event.type() == EventType.SOS ? CHANNEL_SOS : CHANNEL_NORMAL) + event.topic();
        redis.convertAndSend(channel, codec.encode(event));
    }

    @Override
    public void onLocalSubscribe(String topic) {
        if (subscribedTopics.add(topic)) {
            normalContainer.addMessageListener(messageListener, new ChannelTopic(CHANNEL_NORMAL + topic));
            sosContainer.addMessageListener(messageListener, new ChannelTopic(CHANNEL_SOS + topic));
        }
    }

    @Override
    public void onLocalUnsubscribe(String topic) {
        if (subscribedTopics.remove(topic)) {
            normalContainer.removeMessageListener(messageListener, new ChannelTopic(CHANNEL_NORMAL + topic));
            sosContainer.removeMessageListener(messageListener, new ChannelTopic(CHANNEL_SOS + topic));
        }
    }

    @Override
    public String nodeId() {
        return nodeId;
    }

    @Override
    public String describe() {
        return "redis pub/sub (multi-node), separate SOS channel; topics subscribed on this node: "
                + subscribedTopics.size();
    }

    public int subscribedTopicCount() {
        return subscribedTopics.size();
    }
}
