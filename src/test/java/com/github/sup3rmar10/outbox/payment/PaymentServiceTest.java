package com.github.sup3rmar10.outbox.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.sup3rmar10.outbox.MutableClock;
import com.github.sup3rmar10.outbox.TestDb;
import com.github.sup3rmar10.outbox.error.ConflictException;
import com.github.sup3rmar10.outbox.error.NotFoundException;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.NewEvent;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Row;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Status;
import com.github.sup3rmar10.outbox.outbox.OutboxRepository;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.transaction.support.TransactionTemplate;

class PaymentServiceTest {

    private final MutableClock clock = new MutableClock(1_000_000);
    private final OutboxRepository outbox = TestDb.repo();
    private PaymentService svc;

    @BeforeEach
    void setUp() {
        TestDb.reset();
        svc = TestDb.payments(clock);
    }

    private long paymentCount() {
        return TestDb.JDBC.queryForObject("SELECT COUNT(*) FROM payment", Long.class);
    }

    private long outboxCount() {
        return TestDb.JDBC.queryForObject("SELECT COUNT(*) FROM outbox", Long.class);
    }

    @Test
    void createWritesThePaymentAndItsOutboxRowTogether() {
        Payment p = svc.create("acct-1", 2500, "USD");
        assertThat(p.id()).isEqualTo("pmt-1");
        assertThat(p.status()).isEqualTo(PaymentStatus.CREATED);
        assertThat(p.eventSeq()).isEqualTo(1);
        assertThat(paymentCount()).isEqualTo(1);
        List<Row> rows = outbox.findByAggregate("pmt-1");
        assertThat(rows).hasSize(1);
        Row r = rows.get(0);
        assertThat(r.eventType()).isEqualTo("PaymentCreated");
        assertThat(r.aggregateSeq()).isEqualTo(1);
        assertThat(r.idempotencyKey()).isEqualTo("pmt-1:1");
        assertThat(r.status()).isEqualTo(Status.NEW);
    }

    @Test
    void theEventPayloadCarriesTheBusinessFacts() throws Exception {
        svc.create("acct-9", 1999, "EUR");
        Row r = outbox.findByAggregate("pmt-1").get(0);
        var body = TestDb.JSON.readTree(r.payload());
        assertThat(body.get("paymentId").asText()).isEqualTo("pmt-1");
        assertThat(body.get("accountId").asText()).isEqualTo("acct-9");
        assertThat(body.get("amountMinor").asLong()).isEqualTo(1999);
        assertThat(body.get("currency").asText()).isEqualTo("EUR");
        assertThat(body.get("status").asText()).isEqualTo("CREATED");
    }

    @Test
    void everyTransitionAppendsTheNextSequenceNumberWithoutGapsOrRepeats() {
        svc.create("acct-1", 100, "USD");
        svc.capture("pmt-1");
        Payment refunded = svc.refund("pmt-1");
        assertThat(refunded.status()).isEqualTo(PaymentStatus.REFUNDED);
        List<Row> rows = outbox.findByAggregate("pmt-1");
        assertThat(rows).extracting(Row::aggregateSeq).containsExactly(1L, 2L, 3L);
        assertThat(rows).extracting(Row::eventType)
                .containsExactly("PaymentCreated", "PaymentCaptured", "PaymentRefunded");
        assertThat(rows).extracting(Row::idempotencyKey).containsExactly("pmt-1:1", "pmt-1:2", "pmt-1:3");
    }

    @Test
    void aRefusedTransitionChangesNeitherTheStatusNorTheOutbox() {
        svc.create("acct-1", 100, "USD");
        svc.capture("pmt-1");
        assertThatThrownBy(() -> svc.capture("pmt-1")).isInstanceOf(ConflictException.class);
        assertThat(svc.get("pmt-1").status()).isEqualTo(PaymentStatus.CAPTURED);
        assertThat(svc.get("pmt-1").eventSeq()).isEqualTo(2);
        assertThat(outbox.findByAggregate("pmt-1")).hasSize(2);
    }

    @Test
    void refundNeedsACaptureFirstAndCannotRepeat() {
        svc.create("acct-1", 100, "USD");
        assertThatThrownBy(() -> svc.refund("pmt-1")).isInstanceOf(ConflictException.class)
                .hasMessageContaining("CREATED");
        svc.capture("pmt-1");
        svc.refund("pmt-1");
        assertThatThrownBy(() -> svc.refund("pmt-1")).isInstanceOf(ConflictException.class);
        assertThat(outbox.findByAggregate("pmt-1")).hasSize(3);
    }

    @Test
    void unknownPaymentsAreNotFound() {
        assertThatThrownBy(() -> svc.get("nope")).isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> svc.capture("nope")).isInstanceOf(NotFoundException.class);
        assertThatThrownBy(() -> svc.refund("nope")).isInstanceOf(NotFoundException.class);
        assertThat(outboxCount()).isZero();
    }

    @Test
    void invalidInputIsRejectedBeforeAnythingIsWritten() {
        assertThatThrownBy(() -> svc.create("", 100, "USD")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> svc.create(null, 100, "USD")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> svc.create("a".repeat(65), 100, "USD")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> svc.create("a", 0, "USD")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> svc.create("a", -5, "USD")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> svc.create("a", 5, "usd")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> svc.create("a", 5, "US")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> svc.create("a", 5, null)).isInstanceOf(IllegalArgumentException.class);
        assertThat(paymentCount()).isZero();
        assertThat(outboxCount()).isZero();
    }

    @Test
    void timestampsComeFromTheInjectedClock() {
        svc.create("acct-1", 100, "USD");
        clock.advance(5000);
        svc.capture("pmt-1");
        assertThat(svc.get("pmt-1").createdAt()).isEqualTo(1_000_000);
        List<Row> rows = outbox.findByAggregate("pmt-1");
        assertThat(rows.get(0).createdAt()).isEqualTo(1_000_000);
        assertThat(rows.get(0).nextAttemptAt()).isEqualTo(1_000_000);
        assertThat(rows.get(1).createdAt()).isEqualTo(1_005_000);
    }

    @Test
    void idempotencyKeyIsAggregateIdPlusSequence() {
        assertThat(PaymentService.idempotencyKey("pmt-7", 3)).isEqualTo("pmt-7:3");
    }

    // ---- the atomicity guarantee: the payment row and the outbox row commit or roll back together ----

    @Test
    void ifTheOutboxInsertFailsThePaymentIsNotPersisted() {
        // an outbox row already owns the key the new payment's first event will use
        outbox.insert(new NewEvent("other", 1, "X", "{}", "pmt-1:1"), 1);
        assertThatThrownBy(() -> svc.create("acct-1", 100, "USD")).isInstanceOf(DuplicateKeyException.class);
        assertThat(paymentCount()).as("payment row must have rolled back").isZero();
        assertThat(outboxCount()).as("only the pre-existing row remains").isEqualTo(1);
    }

    @Test
    void ifTheCallersTransactionFailsAfterBothWritesNeitherPersists() {
        TransactionTemplate outer = new TransactionTemplate(TestDb.TM);
        assertThatThrownBy(() -> outer.executeWithoutResult(status -> {
            svc.create("acct-1", 100, "USD");
            assertThat(paymentCount()).as("visible inside the transaction").isEqualTo(1);
            assertThat(outboxCount()).as("visible inside the transaction").isEqualTo(1);
            throw new IllegalStateException("something later in the unit of work failed");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(paymentCount()).isZero();
        assertThat(outboxCount()).isZero();
    }

    @Test
    void aRolledBackTransitionLeavesStatusAndOutboxAsTheyWere() {
        svc.create("acct-1", 100, "USD");
        TransactionTemplate outer = new TransactionTemplate(TestDb.TM);
        assertThatThrownBy(() -> outer.executeWithoutResult(status -> {
            svc.capture("pmt-1");
            throw new IllegalStateException("boom");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(svc.get("pmt-1").status()).isEqualTo(PaymentStatus.CREATED);
        assertThat(svc.get("pmt-1").eventSeq()).isEqualTo(1);
        assertThat(outbox.findByAggregate("pmt-1")).hasSize(1);
    }

    @Test
    void whenTheCallersTransactionCommitsBothRowsAppearTogether() {
        TransactionTemplate outer = new TransactionTemplate(TestDb.TM);
        outer.executeWithoutResult(status -> {
            svc.create("acct-1", 100, "USD");
            svc.capture("pmt-1");
        });
        assertThat(paymentCount()).isEqualTo(1);
        assertThat(outboxCount()).isEqualTo(2);
    }
}
