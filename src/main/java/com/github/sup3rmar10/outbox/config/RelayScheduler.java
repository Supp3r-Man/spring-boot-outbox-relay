package com.github.sup3rmar10.outbox.config;

import com.github.sup3rmar10.outbox.outbox.OutboxRelay;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** Runs the relay in the background. Switch off with outbox.scheduler-enabled=false (tests do, to stay deterministic). */
@Component
@ConditionalOnProperty(name = "outbox.scheduler-enabled", havingValue = "true", matchIfMissing = true)
public class RelayScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(RelayScheduler.class);

    private final OutboxRelay relay;

    public RelayScheduler(OutboxRelay relay) {
        this.relay = relay;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-millis:500}")
    public void poll() {
        try {
            relay.pollOnce();
        } catch (RuntimeException e) {
            LOG.warn("relay poll failed; will retry on the next tick", e);
        }
    }
}
