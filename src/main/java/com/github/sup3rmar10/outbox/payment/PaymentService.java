package com.github.sup3rmar10.outbox.payment;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sup3rmar10.outbox.error.ConflictException;
import com.github.sup3rmar10.outbox.error.NotFoundException;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.NewEvent;
import com.github.sup3rmar10.outbox.outbox.OutboxRepository;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The producer side of the pattern: every state change writes the payment row AND its outbox row inside ONE
 * database transaction. Nothing here talks to a broker.
 */
public class PaymentService {

    private static final Pattern CURRENCY = Pattern.compile("[A-Z]{3}");

    private final JdbcTemplate jdbc;
    private final OutboxRepository outbox;
    private final TransactionTemplate tx;
    private final Clock clock;
    private final ObjectMapper json;
    private final Supplier<String> ids;

    public PaymentService(JdbcTemplate jdbc, OutboxRepository outbox, PlatformTransactionManager tm, Clock clock,
                          ObjectMapper json, Supplier<String> ids) {
        this.jdbc = jdbc;
        this.outbox = outbox;
        this.tx = new TransactionTemplate(tm);
        // Join the caller's transaction if there is one; start one if not. REQUIRES_NEW or SUPPORTS would break the guarantee.
        this.tx.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRED);
        this.clock = clock;
        this.json = json;
        this.ids = ids;
    }

    public PaymentService(JdbcTemplate jdbc, OutboxRepository outbox, PlatformTransactionManager tm, Clock clock,
                          ObjectMapper json) {
        this(jdbc, outbox, tm, clock, json, () -> "pmt-" + UUID.randomUUID());
    }

    public Payment create(String accountId, long amountMinor, String currency) {
        if (accountId == null || accountId.isBlank() || accountId.length() > 64) {
            throw new IllegalArgumentException("accountId must be 1..64 characters");
        }
        if (amountMinor <= 0) {
            throw new IllegalArgumentException("amountMinor must be positive");
        }
        if (currency == null || !CURRENCY.matcher(currency).matches()) {
            throw new IllegalArgumentException("currency must be three upper-case letters");
        }
        String id = ids.get();
        return tx.execute(status -> {
            long now = clock.millis();
            jdbc.update("INSERT INTO payment (id, account_id, amount_minor, currency, status, event_seq, created_at)"
                    + " VALUES (?, ?, ?, ?, 'CREATED', 1, ?)", id, accountId, amountMinor, currency, now);
            Payment p = get(id);
            emit(p, "PaymentCreated", now);
            return p;
        });
    }

    public Payment capture(String id) {
        return transition(id, PaymentStatus.CREATED, PaymentStatus.CAPTURED, "PaymentCaptured");
    }

    public Payment refund(String id) {
        return transition(id, PaymentStatus.CAPTURED, PaymentStatus.REFUNDED, "PaymentRefunded");
    }

    public Payment get(String id) {
        List<Payment> found = jdbc.query("SELECT * FROM payment WHERE id = ?", (rs, i) -> new Payment(
                rs.getString("id"), rs.getString("account_id"), rs.getLong("amount_minor"),
                rs.getString("currency"), PaymentStatus.valueOf(rs.getString("status")),
                rs.getLong("event_seq"), rs.getLong("created_at")), id);
        if (found.isEmpty()) {
            throw new NotFoundException("payment " + id + " not found");
        }
        return found.get(0);
    }

    private Payment transition(String id, PaymentStatus from, PaymentStatus to, String eventType) {
        return tx.execute(status -> {
            // The guarded UPDATE locks the payment row, so concurrent transitions of one payment serialise and
            // event_seq (the per-aggregate sequence) is handed out exactly once per change.
            int n = jdbc.update("UPDATE payment SET status = ?, event_seq = event_seq + 1 WHERE id = ? AND status = ?",
                    to.name(), id, from.name());
            if (n == 0) {
                Payment current = get(id);
                throw new ConflictException("payment " + id + " is " + current.status() + ", expected " + from);
            }
            Payment p = get(id);
            emit(p, eventType, clock.millis());
            return p;
        });
    }

    private void emit(Payment p, String eventType, long now) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("paymentId", p.id());
        body.put("accountId", p.accountId());
        body.put("amountMinor", p.amountMinor());
        body.put("currency", p.currency());
        body.put("status", p.status().name());
        String payload;
        try {
            payload = json.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException(e);
        }
        outbox.insert(new NewEvent(p.id(), p.eventSeq(), eventType, payload, idempotencyKey(p.id(), p.eventSeq())), now);
    }

    /** Stable and derived from the domain change, so a requeue or a crash replay carries the same key. */
    public static String idempotencyKey(String aggregateId, long seq) {
        return aggregateId + ":" + seq;
    }
}
