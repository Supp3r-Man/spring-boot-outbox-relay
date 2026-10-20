package com.github.sup3rmar10.outbox.outbox;

public record RelayConfig(
        String relayId, int batchSize, long leaseMillis, int maxAttempts, long baseBackoffMillis, long maxBackoffMillis) {

    public RelayConfig {
        if (relayId == null || relayId.isBlank() || relayId.length() > 40) {
            throw new IllegalArgumentException("relayId must be 1..40 characters");
        }
        if (batchSize < 1) {
            throw new IllegalArgumentException("batchSize must be >= 1");
        }
        if (leaseMillis < 1) {
            throw new IllegalArgumentException("leaseMillis must be >= 1");
        }
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (baseBackoffMillis < 0 || maxBackoffMillis < baseBackoffMillis) {
            throw new IllegalArgumentException("need 0 <= baseBackoffMillis <= maxBackoffMillis");
        }
    }
}
