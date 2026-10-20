package com.github.sup3rmar10.outbox.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.sup3rmar10.outbox.MutableClock;
import com.github.sup3rmar10.outbox.TestDb;
import com.github.sup3rmar10.outbox.consumer.IdempotentConsumer;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Message;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Metrics;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.PollResult;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Publisher;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Row;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Status;
import com.github.sup3rmar10.outbox.payment.PaymentService;
import com.github.sup3rmar10.outbox.publish.InMemoryBroker;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Relay behaviour on a fake clock: every timing assertion is exact, nothing sleeps. */
class OutboxRelayTest {

    private static final long LEASE = 30_000;

    /** Stands in for a JVM that dies after publish and before the row is marked. */
    static final class SimulatedCrash extends RuntimeException {
        SimulatedCrash() {
            super("relay process died");
        }
    }

    private final MutableClock clock = new MutableClock(1_000_000);
    private final OutboxRepository repo = TestDb.repo();
    private final InMemoryBroker broker = new InMemoryBroker();
    private PaymentService payments;
    private OutboxRelay relay;

    @BeforeEach
    void setUp() {
        TestDb.reset();
        payments = TestDb.payments(clock);
        relay = relay("r1", 10, broker);
    }

    private OutboxRelay relay(String id, int batch, Publisher target) {
        return new OutboxRelay(repo, target, clock, new RelayConfig(id, batch, LEASE, 3, 1000, 8000));
    }

    private Row row(String aggregate, long seq) {
        return repo.findByAggregate(aggregate).stream().filter(r -> r.aggregateSeq() == seq).findFirst().orElseThrow();
    }

    @Test
    void pollPublishesDueRowsAndMarksThemPublished() {
        payments.create("acct-1", 100, "USD");
        payments.create("acct-2", 200, "USD");
        PollResult result = relay.pollOnce();
        assertThat(result).isEqualTo(new PollResult(2, 2, 0, 0, 0));
        assertThat(broker.deliveredKeys()).containsExactly("pmt-1:1", "pmt-2:1");
        assertThat(row("pmt-1", 1).status()).isEqualTo(Status.PUBLISHED);
        assertThat(row("pmt-1", 1).publishedAt()).isEqualTo(1_000_000);
    }

    @Test
    void anEmptyOutboxPollsToNothing() {
        assertThat(relay.pollOnce()).isEqualTo(new PollResult(0, 0, 0, 0, 0));
        assertThat(broker.delivered()).isEmpty();
    }

    @Test
    void aPublishedRowIsNeverPublishedAgain() {
        payments.create("acct-1", 100, "USD");
        relay.pollOnce();
        clock.advance(10 * LEASE);
        assertThat(relay.pollOnce().claimed()).isZero();
        assertThat(broker.delivered()).hasSize(1);
    }

    @Test
    void batchSizeLimitsTheRowsHandledPerPoll() {
        for (int i = 0; i < 5; i++) {
            payments.create("acct-1", 100 + i, "USD");
        }
        OutboxRelay small = relay("r1", 2, broker);
        assertThat(small.pollOnce().published()).isEqualTo(2);
        assertThat(small.pollOnce().published()).isEqualTo(2);
        assertThat(small.pollOnce().published()).isEqualTo(1);
        assertThat(small.pollOnce().claimed()).isZero();
        assertThat(broker.deliveredKeys()).containsExactly("pmt-1:1", "pmt-2:1", "pmt-3:1", "pmt-4:1", "pmt-5:1");
    }

    @Test
    void theMessageCarriesTheIdempotencyKeyAndTheAttemptNumber() {
        payments.create("acct-1", 100, "USD");
        broker.failNext(1);
        relay.pollOnce();
        clock.advance(1000);
        relay.pollOnce();
        Message m = broker.delivered().get(0);
        assertThat(m.idempotencyKey()).isEqualTo("pmt-1:1");
        assertThat(m.aggregateId()).isEqualTo("pmt-1");
        assertThat(m.aggregateSeq()).isEqualTo(1);
        assertThat(m.eventType()).isEqualTo("PaymentCreated");
        assertThat(m.attempt()).isEqualTo(2);
        assertThat(m.payload()).contains("\"amountMinor\":100");
    }

    @Test
    void aFailedPublishSchedulesARetryAfterTheBaseBackoff() {
        payments.create("acct-1", 100, "USD");
        broker.failNext(1);
        PollResult result = relay.pollOnce();
        assertThat(result).isEqualTo(new PollResult(1, 0, 1, 0, 0));
        Row r = row("pmt-1", 1);
        assertThat(r.status()).isEqualTo(Status.NEW);
        assertThat(r.attempts()).isEqualTo(1);
        assertThat(r.nextAttemptAt()).isEqualTo(1_001_000);
        assertThat(r.lastError()).contains("broker unavailable");
    }

    @Test
    void theRetryWaitsForTheFullBackoffAndNotOneMillisecondLess() {
        payments.create("acct-1", 100, "USD");
        broker.failNext(1);
        relay.pollOnce();
        clock.advance(999);
        assertThat(relay.pollOnce().claimed()).isZero();
        clock.advance(1);
        assertThat(relay.pollOnce()).isEqualTo(new PollResult(1, 1, 0, 0, 0));
        assertThat(broker.delivered()).hasSize(1);
    }

    @Test
    void backoffDoublesOnEveryConsecutiveFailure() {
        payments.create("acct-1", 100, "USD");
        broker.failNext(2);
        relay.pollOnce();
        assertThat(row("pmt-1", 1).nextAttemptAt()).isEqualTo(1_000_000 + 1000);
        clock.advance(1000);
        relay.pollOnce();
        assertThat(row("pmt-1", 1).nextAttemptAt()).isEqualTo(1_001_000 + 2000);
        assertThat(row("pmt-1", 1).attempts()).isEqualTo(2);
    }

    @Test
    void afterMaxAttemptsTheRowIsDeadAndKeepsTheLastError() {
        payments.create("acct-1", 100, "USD");
        broker.failNext(99);
        relay.pollOnce();
        clock.advance(1000);
        relay.pollOnce();
        clock.advance(2000);
        PollResult last = relay.pollOnce();
        assertThat(last).isEqualTo(new PollResult(1, 0, 0, 1, 0));
        Row r = row("pmt-1", 1);
        assertThat(r.status()).isEqualTo(Status.DEAD);
        assertThat(r.attempts()).isEqualTo(3);
        assertThat(r.lastError()).contains("broker unavailable");
        clock.advance(10 * LEASE);
        assertThat(relay.pollOnce().claimed()).as("dead rows are not retried").isZero();
    }

    @Test
    void aRuntimeExceptionFromThePublisherIsTreatedLikeAFailure() {
        payments.create("acct-1", 100, "USD");
        Publisher exploding = m -> {
            throw new IllegalStateException("serializer blew up");
        };
        PollResult result = relay("r1", 10, exploding).pollOnce();
        assertThat(result.retried()).isEqualTo(1);
        assertThat(row("pmt-1", 1).lastError()).contains("serializer blew up");
    }

    @Test
    void aVeryLongErrorMessageIsTruncatedToFitTheColumn() {
        payments.create("acct-1", 100, "USD");
        Publisher exploding = m -> {
            throw new IllegalStateException("x".repeat(2000));
        };
        relay("r1", 10, exploding).pollOnce();
        assertThat(row("pmt-1", 1).lastError()).hasSize(500);
    }

    // ---- ordering ----

    @Test
    void aLaterEventIsNotPublishedWhileAnEarlierOneOfTheSamePaymentIsFailing() {
        payments.create("acct-1", 100, "USD");
        payments.capture("pmt-1");
        broker.failNext(1);
        relay.pollOnce();
        assertThat(broker.delivered()).isEmpty();
        // the capture event is due, but it waits behind the creation event that is in backoff
        assertThat(relay.pollOnce().claimed()).isZero();
        clock.advance(1000);
        relay.pollOnce();
        assertThat(broker.deliveredKeys()).containsExactly("pmt-1:1");
        relay.pollOnce();
        assertThat(broker.deliveredKeys()).containsExactly("pmt-1:1", "pmt-1:2");
    }

    @Test
    void otherPaymentsKeepFlowingWhileOneIsStuck() {
        payments.create("acct-1", 100, "USD");
        payments.create("acct-2", 200, "USD");
        broker.failWhen(m -> m.aggregateId().equals("pmt-1"));
        PollResult result = relay.pollOnce();
        assertThat(result.published()).isEqualTo(1);
        assertThat(result.retried()).isEqualTo(1);
        assertThat(broker.deliveredKeys()).containsExactly("pmt-2:1");
    }

    @Test
    void perAggregateOrderHoldsWithSmallBatchesAndMixedFailures() {
        for (int i = 0; i < 4; i++) {
            payments.create("acct-" + i, 100, "USD");
        }
        for (int i = 1; i <= 4; i++) {
            payments.capture("pmt-" + i);
            payments.refund("pmt-" + i);
        }
        // every third publish call fails (counted by a wrapper), the clock moves on every round
        int[] calls = {0};
        Publisher flaky = m -> {
            if (++calls[0] % 3 == 0) {
                throw new OutboxModel.PublishException("flaky");
            }
            broker.publish(m);
        };
        OutboxRelay flakyRelay = new OutboxRelay(repo, flaky, clock, new RelayConfig("r1", 3, LEASE, 50, 1000, 8000));
        for (int round = 0; round < 60 && repo.countUnpublished() > 0; round++) {
            flakyRelay.pollOnce();
            clock.advance(8000);
        }
        assertThat(repo.countUnpublished()).isZero();
        Map<String, List<Long>> seqs = new LinkedHashMap<>();
        for (Message m : broker.delivered()) {
            seqs.computeIfAbsent(m.aggregateId(), k -> new ArrayList<>()).add(m.aggregateSeq());
        }
        assertThat(seqs).hasSize(4);
        seqs.values().forEach(list -> assertThat(list).containsExactly(1L, 2L, 3L));
    }

    @Test
    void aDeadEventBlocksItsSuccessorsUntilItIsRequeued() {
        payments.create("acct-1", 100, "USD");
        payments.capture("pmt-1");
        payments.create("acct-2", 200, "USD");
        broker.failWhen(m -> m.idempotencyKey().equals("pmt-1:1"));
        for (int i = 0; i < 3; i++) {
            relay.pollOnce();
            clock.advance(10_000);
        }
        assertThat(row("pmt-1", 1).status()).isEqualTo(Status.DEAD);
        assertThat(row("pmt-1", 2).status()).as("successor waits behind the dead row").isEqualTo(Status.NEW);
        assertThat(broker.deliveredKeys()).containsExactly("pmt-2:1");

        broker.failWhen(null);
        assertThat(repo.requeueAllDead(clock.millis())).isEqualTo(1);
        relay.pollOnce();
        relay.pollOnce();
        assertThat(broker.deliveredKeys()).containsExactly("pmt-2:1", "pmt-1:1", "pmt-1:2");
    }

    @Test
    void aRequeuedRowKeepsItsIdempotencyKeyAndGetsAFreshAttemptBudget() {
        payments.create("acct-1", 100, "USD");
        broker.failNext(99);
        for (int i = 0; i < 3; i++) {
            relay.pollOnce();
            clock.advance(10_000);
        }
        assertThat(row("pmt-1", 1).status()).isEqualTo(Status.DEAD);
        broker.failNext(0);
        repo.requeueAllDead(clock.millis());
        relay.pollOnce();
        Message m = broker.delivered().get(0);
        assertThat(m.idempotencyKey()).isEqualTo("pmt-1:1");
        assertThat(m.attempt()).as("attempt counter restarted").isEqualTo(1);
    }

    // ---- claiming, leases, crashes ----

    @Test
    void theLeaseOwnerNamesTheRelayAndTheLeaseEndsAfterTheConfiguredTime() {
        payments.create("acct-1", 100, "USD");
        List<Row> seen = new ArrayList<>();
        new OutboxRelay(repo, broker, clock, new RelayConfig("r7", 10, LEASE, 3, 1000, 8000), seen::add).pollOnce();
        assertThat(seen).hasSize(1);
        assertThat(seen.get(0).leaseOwner()).startsWith("r7#");
        assertThat(seen.get(0).leaseUntil()).isEqualTo(1_000_000 + LEASE);
    }

    @Test
    void twoRelaysPollingOneAfterTheOtherPublishEachRowOnce() {
        for (int i = 0; i < 6; i++) {
            payments.create("acct-1", 100 + i, "USD");
        }
        OutboxRelay a = relay("ra", 3, broker);
        OutboxRelay b = relay("rb", 3, broker);
        assertThat(a.pollOnce().published()).isEqualTo(3);
        assertThat(b.pollOnce().published()).isEqualTo(3);
        assertThat(a.pollOnce().claimed()).isZero();
        assertThat(broker.deliveredKeys()).doesNotHaveDuplicates().hasSize(6);
    }

    @Test
    void aCrashBetweenPublishAndMarkCausesADuplicateDeliveryThatTheConsumerDedupes() {
        IdempotentConsumer consumer = TestDb.consumer(clock);
        Publisher brokerThenConsumer = m -> {
            broker.publish(m);
            consumer.handle(m);
        };
        payments.create("acct-1", 700, "USD");
        payments.capture("pmt-1");

        OutboxRelay doomed = new OutboxRelay(repo, brokerThenConsumer, clock,
                new RelayConfig("doomed", 10, LEASE, 3, 1000, 8000), row -> {
                    throw new SimulatedCrash();
                });
        assertThatThrownBy(doomed::pollOnce).isInstanceOf(SimulatedCrash.class);
        // the broker already has the message, but the outbox never learned that
        assertThat(broker.deliveredKeys()).containsExactly("pmt-1:1");
        assertThat(row("pmt-1", 1).status()).isEqualTo(Status.CLAIMED);

        OutboxRelay survivor = relay("survivor", 10, brokerThenConsumer);
        assertThat(survivor.pollOnce().claimed()).as("lease still valid, nobody may take it").isZero();
        clock.advance(LEASE);
        survivor.pollOnce(); // pmt-1:1 again (duplicate)
        survivor.pollOnce(); // then pmt-1:2

        assertThat(broker.deliveredKeys()).containsExactly("pmt-1:1", "pmt-1:1", "pmt-1:2");
        assertThat(row("pmt-1", 1).attempts()).as("a crashed attempt counts as an attempt").isEqualTo(2);
        assertThat(consumer.processedCount()).isEqualTo(2);
        assertThat(consumer.totals()).containsEntry("acct-1", 700L);
        assertThat(consumer.eventsApplied()).isEqualTo(1);
        assertThat(repo.countUnpublished()).isZero();
    }

    @Test
    void aRelayRestartedWithTheSameIdDoesNotReprocessRowsItsPreviousRunStillHasLeased() {
        payments.create("acct-1", 100, "USD");
        payments.create("acct-2", 200, "USD");
        OutboxRelay doomed = new OutboxRelay(repo, broker, clock, new RelayConfig("same-id", 10, LEASE, 3, 1000, 8000),
                row -> {
                    throw new SimulatedCrash();
                });
        assertThatThrownBy(doomed::pollOnce).isInstanceOf(SimulatedCrash.class);
        assertThat(broker.deliveredKeys()).containsExactly("pmt-1:1");

        payments.create("acct-3", 300, "USD");
        PollResult restarted = relay("same-id", 10, broker).pollOnce();
        // only the new row: the two rows still under the old run's lease are not ours to touch yet
        assertThat(restarted).isEqualTo(new PollResult(1, 1, 0, 0, 0));
        assertThat(broker.deliveredKeys()).containsExactly("pmt-1:1", "pmt-3:1");
    }

    @Test
    void aRelayThatOutlivesItsLeaseLosesTheFinalWriteWhileTheNewOwnerPublishes() {
        payments.create("acct-1", 100, "USD");
        OutboxRelay[] rival = new OutboxRelay[1];
        PollResult[] rivalResult = new PollResult[1];
        // while relay "slow" is inside publish, its lease runs out and another relay takes the row
        Publisher slowBroker = m -> {
            clock.advance(LEASE);
            if (rival[0] != null) {
                OutboxRelay r = rival[0];
                rival[0] = null;
                rivalResult[0] = r.pollOnce();
            }
            broker.publish(m);
        };
        rival[0] = relay("rival", 10, slowBroker);
        PollResult slow = relay("slow", 10, slowBroker).pollOnce();
        assertThat(rivalResult[0].published()).isEqualTo(1);
        assertThat(slow.published()).isZero();
        assertThat(slow.lostLease()).as("fenced out: the row now belongs to the rival").isEqualTo(1);
        assertThat(broker.deliveredKeys()).as("both published: at-least-once, not exactly-once")
                .containsExactly("pmt-1:1", "pmt-1:1");
        assertThat(row("pmt-1", 1).status()).isEqualTo(Status.PUBLISHED);
    }

    @Test
    void metricsReflectPublishedPendingAndDeadRows() {
        payments.create("acct-1", 100, "USD");
        payments.create("acct-2", 200, "USD");
        payments.create("acct-3", 300, "USD");
        broker.failWhen(m -> m.aggregateId().equals("pmt-2"));
        for (int i = 0; i < 3; i++) {
            relay.pollOnce();
            clock.advance(10_000);
        }
        payments.create("acct-4", 400, "USD");
        Metrics m = repo.metrics(clock.millis());
        assertThat(m.published()).isEqualTo(2);
        assertThat(m.dead()).isEqualTo(1);
        assertThat(m.pending()).isEqualTo(1);
        assertThat(m.inFlight()).isZero();
        assertThat(m.oldestUnpublishedAgeMillis()).isZero();
    }
}
