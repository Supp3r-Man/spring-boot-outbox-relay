package com.github.sup3rmar10.outbox.outbox;

import com.github.sup3rmar10.outbox.outbox.OutboxModel.AfterPublishHook;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Message;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.PollResult;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.PublishException;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Publisher;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Row;
import java.time.Clock;
import java.util.List;
import java.util.UUID;

/**
 * Polling relay. One call to pollOnce() claims a batch, publishes each row, and writes the outcome back.
 *
 * Delivery is at-least-once: a row is marked PUBLISHED only after Publisher.publish returned, so a crash between
 * the two leaves the row CLAIMED; once its lease expires another poll publishes it again (a duplicate with the
 * same idempotency key). Ordering is per aggregate: the claim only offers the lowest unfinished row of each
 * aggregate, and a DEAD row blocks the rows behind it until it is requeued.
 *
 * Not thread-safe per instance by design: run one thread per relay instance (relayId must be unique).
 */
public class OutboxRelay {

    private final OutboxRepository repo;
    private final Publisher publisher;
    private final Clock clock;
    private final RelayConfig config;
    private final AfterPublishHook hook;

    public OutboxRelay(OutboxRepository repo, Publisher publisher, Clock clock, RelayConfig config) {
        this(repo, publisher, clock, config, AfterPublishHook.NONE);
    }

    public OutboxRelay(OutboxRepository repo, Publisher publisher, Clock clock, RelayConfig config,
                       AfterPublishHook hook) {
        this.repo = repo;
        this.publisher = publisher;
        this.clock = clock;
        this.config = config;
        this.hook = hook;
    }

    public PollResult pollOnce() {
        long now = clock.millis();
        String token = config.relayId() + "#" + UUID.randomUUID();
        List<Row> rows = repo.claim(token, now, now + config.leaseMillis(), config.batchSize());
        int published = 0;
        int retried = 0;
        int dead = 0;
        int lost = 0;
        for (Row row : rows) {
            try {
                publisher.publish(toMessage(row));
            } catch (PublishException | RuntimeException e) {
                String error = abbreviate(e.toString());
                if (row.attempts() >= config.maxAttempts()) {
                    if (repo.markDead(row.id(), token, error)) {
                        dead++;
                    } else {
                        lost++;
                    }
                } else {
                    long next = clock.millis()
                            + Backoff.delayMillis(row.attempts(), config.baseBackoffMillis(), config.maxBackoffMillis());
                    if (repo.markRetry(row.id(), token, next, error)) {
                        retried++;
                    } else {
                        lost++;
                    }
                }
                continue;
            }
            hook.afterPublish(row);
            if (repo.markPublished(row.id(), token, clock.millis())) {
                published++;
            } else {
                lost++;
            }
        }
        return new PollResult(rows.size(), published, retried, dead, lost);
    }

    public static Message toMessage(Row row) {
        return new Message(row.id(), row.aggregateId(), row.aggregateSeq(), row.eventType(), row.payload(),
                row.idempotencyKey(), row.attempts());
    }

    static String abbreviate(String s) {
        return s.length() <= 500 ? s : s.substring(0, 500);
    }
}
