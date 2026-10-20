package com.github.sup3rmar10.outbox;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.concurrent.atomic.AtomicLong;

/** A clock that only moves when the test says so. Thread-safe. */
public final class MutableClock extends Clock {
    private final AtomicLong millis;

    public MutableClock(long startMillis) {
        this.millis = new AtomicLong(startMillis);
    }

    public void advance(long deltaMillis) {
        millis.addAndGet(deltaMillis);
    }

    public void advance(Duration d) {
        advance(d.toMillis());
    }

    public void set(long m) {
        millis.set(m);
    }

    @Override
    public ZoneId getZone() {
        return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        return this;
    }

    @Override
    public long millis() {
        return millis.get();
    }

    @Override
    public Instant instant() {
        return Instant.ofEpochMilli(millis.get());
    }
}
