package com.github.sup3rmar10.outbox.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "outbox")
public record OutboxProperties(
        @DefaultValue("relay-1") String relayId,
        @DefaultValue("10") int batchSize,
        @DefaultValue("30000") long leaseMillis,
        @DefaultValue("5") int maxAttempts,
        @DefaultValue("1000") long baseBackoffMillis,
        @DefaultValue("60000") long maxBackoffMillis,
        @DefaultValue("500") long pollIntervalMillis,
        @DefaultValue("true") boolean schedulerEnabled,
        @DefaultValue("memory") String publisher,
        @DefaultValue("outbox-broker.jsonl") String filePath,
        @DefaultValue("true") boolean demoConsumer) {
}
