package com.github.sup3rmar10.outbox.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Message;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Sample consumer that turns at-least-once delivery into effectively-once processing: it records the message's
 * idempotency key (primary key) and applies the business effect in the SAME transaction, so a redelivery either
 * finds the key and does nothing, or loses the race on the primary key and does nothing.
 *
 * Effect: net captured amount per account (+ on PaymentCaptured, - on PaymentRefunded).
 */
public class IdempotentConsumer {

    private final JdbcTemplate jdbc;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final ObjectMapper json;

    public IdempotentConsumer(JdbcTemplate jdbc, PlatformTransactionManager tm, Clock clock, ObjectMapper json) {
        this.jdbc = jdbc;
        this.tx = new TransactionTemplate(tm);
        this.clock = clock;
        this.json = json;
    }

    /** @return true if the message was applied now, false if it was a duplicate. */
    public boolean handle(Message message) {
        Boolean applied = tx.execute(status -> {
            try {
                jdbc.update("INSERT INTO processed_message (idempotency_key, processed_at) VALUES (?, ?)",
                        message.idempotencyKey(), clock.millis());
            } catch (DuplicateKeyException duplicate) {
                return false;
            }
            apply(message);
            return true;
        });
        return Boolean.TRUE.equals(applied);
    }

    private void apply(Message message) {
        long sign = switch (message.eventType()) {
            case "PaymentCaptured" -> 1;
            case "PaymentRefunded" -> -1;
            default -> 0;
        };
        if (sign == 0) {
            return;
        }
        JsonNode body;
        try {
            body = json.readTree(message.payload());
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("malformed payload for " + message.idempotencyKey(), e);
        }
        String account = body.path("accountId").asText();
        long delta = sign * body.path("amountMinor").asLong();
        for (int i = 0; i < 5; i++) {
            if (jdbc.update("UPDATE account_captured SET total_minor = total_minor + ?, events = events + 1"
                    + " WHERE account_id = ?", delta, account) == 1) {
                return;
            }
            try {
                jdbc.update("INSERT INTO account_captured (account_id, total_minor, events) VALUES (?, ?, 1)",
                        account, delta);
                return;
            } catch (DuplicateKeyException lostInsertRace) {
                // another message created the account row first; loop and update it
            }
        }
        throw new IllegalStateException("could not update account_captured for " + account);
    }

    public long processedCount() {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM processed_message", Long.class);
        return n == null ? 0 : n;
    }

    public Map<String, Long> totals() {
        Map<String, Long> out = new LinkedHashMap<>();
        jdbc.query("SELECT account_id, total_minor FROM account_captured ORDER BY account_id",
                rs -> { out.put(rs.getString(1), rs.getLong(2)); });
        return out;
    }

    public long eventsApplied() {
        Long n = jdbc.queryForObject("SELECT COALESCE(SUM(events), 0) FROM account_captured", Long.class);
        return n == null ? 0 : n;
    }
}
