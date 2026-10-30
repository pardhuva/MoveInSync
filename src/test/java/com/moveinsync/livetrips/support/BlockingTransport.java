package com.moveinsync.livetrips.support;

import com.moveinsync.livetrips.gateway.ClientTransport;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A deliberately frozen consumer: its {@code send} blocks until released, modelling the "security
 * desk browser freezes" scenario. Used by the failure-injection test to prove a slow consumer
 * cannot stall the delivery pipeline for everyone else.
 */
public class BlockingTransport implements ClientTransport {

    private final String id;
    private final CountDownLatch release = new CountDownLatch(1);
    private final AtomicBoolean open = new AtomicBoolean(true);
    private final AtomicInteger sendsStarted = new AtomicInteger();

    public BlockingTransport(String id) {
        this.id = id;
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public void send(String payload) throws InterruptedException {
        // Allow subscription handshake and snapshot control frames to pass without blocking.
        if (isControlFrame(payload)) {
            return;
        }
        sendsStarted.incrementAndGet();
        // Block the writer thread here, as a real stuck TCP send would.
        release.await(30, TimeUnit.SECONDS);
        if (!open.get()) {
            throw new IllegalStateException("closed while blocked");
        }
    }

    private boolean isControlFrame(String payload) {
        if (payload == null) {
            return false;
        }
        return payload.contains("\"kind\":\"subscribed\"")
                || payload.contains("\"kind\":\"snapshot\"")
                || payload.contains("\"action\":\"subscribed\"")
                || payload.contains("\"type\":\"SNAPSHOT\"");
    }

    /** Let the frozen consumer resume. */
    public void unblock() {
        release.countDown();
    }

    public int sendsStarted() {
        return sendsStarted.get();
    }

    @Override
    public void close(String reason) {
        open.set(false);
        release.countDown();
    }

    @Override
    public boolean isOpen() {
        return open.get();
    }
}
