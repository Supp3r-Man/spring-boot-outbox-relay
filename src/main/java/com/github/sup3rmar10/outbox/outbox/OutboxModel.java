package com.github.sup3rmar10.outbox.outbox;

/**
 * Small value types of the outbox, grouped in one file to keep the package readable.
 * Referenced as {@code OutboxModel.Row}, {@code OutboxModel.Message}, and so on.
 */
public final class OutboxModel {
    private OutboxModel() {
    }

    /** NEW (waiting, possibly in backoff) -> CLAIMED (leased to one relay) -> PUBLISHED, or back to NEW, or DEAD. */
    public enum Status {
        NEW, CLAIMED, PUBLISHED, DEAD
    }

    public record Row(
            long id,
            String aggregateId,
            long aggregateSeq,
            String eventType,
            String payload,
            String idempotencyKey,
            Status status,
            int attempts,
            long nextAttemptAt,
            String leaseOwner,
            Long leaseUntil,
            String lastError,
            long createdAt,
            Long publishedAt) {
    }

    /** What the domain code hands to the outbox inside its own transaction. */
    public record NewEvent(String aggregateId, long aggregateSeq, String eventType, String payload, String idempotencyKey) {
    }

    /** What a Publisher receives. Consumers dedupe on idempotencyKey; attempt is informational only. */
    public record Message(
            long outboxId,
            String aggregateId,
            long aggregateSeq,
            String eventType,
            String payload,
            String idempotencyKey,
            int attempt) {
    }

    /** pending = NEW (including rows waiting out a backoff), inFlight = CLAIMED. Age is null when nothing is unpublished. */
    public record Metrics(long pending, long inFlight, long published, long dead, Long oldestUnpublishedAgeMillis) {
    }

    /** lostLease counts rows whose final state write was refused because another relay now owns the row. */
    public record PollResult(int claimed, int published, int retried, int dead, int lostLease) {
    }

    /**
     * Test seam that runs after a successful publish and before the row is marked PUBLISHED. Throwing from it
     * simulates a relay crash in exactly the window that makes at-least-once delivery produce duplicates.
     */
    @FunctionalInterface
    public interface AfterPublishHook {
        AfterPublishHook NONE = row -> { };

        void afterPublish(Row row);
    }

    /**
     * Pluggable sink for outbox messages. Implementations here: an in-memory test broker and a JSON-lines file.
     * There is NO Kafka or RabbitMQ implementation in this repo. A real one must throw on failure, never swallow
     * it, because the relay marks the row PUBLISHED as soon as publish returns normally.
     */
    public interface Publisher {
        void publish(Message message) throws PublishException;
    }

    public static class PublishException extends Exception {
        public PublishException(String message) {
            super(message);
        }

        public PublishException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
