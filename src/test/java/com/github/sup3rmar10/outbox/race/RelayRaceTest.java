package com.github.sup3rmar10.outbox.race;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sup3rmar10.outbox.TestDb;
import com.github.sup3rmar10.outbox.consumer.IdempotentConsumer;
import com.github.sup3rmar10.outbox.error.ConflictException;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Message;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.PublishException;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Publisher;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Row;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Status;
import com.github.sup3rmar10.outbox.outbox.OutboxRelay;
import com.github.sup3rmar10.outbox.outbox.OutboxRepository;
import com.github.sup3rmar10.outbox.outbox.RelayConfig;
import com.github.sup3rmar10.outbox.payment.PaymentService;
import com.github.sup3rmar10.outbox.publish.InMemoryBroker;
import java.time.Clock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Real threads, real pooled H2 connections, real clock, no sleeping to "let things happen". These assert
 * invariants, not a particular interleaving. A race that goes the lucky way would still pass, so a mutation
 * killed only by this class is killed probabilistically (see the README).
 */
class RelayRaceTest {

    private static final int PAYMENTS = 60;

    private final Clock clock = Clock.systemUTC();
    private final OutboxRepository repo = TestDb.repo();
    private final InMemoryBroker broker = new InMemoryBroker();
    private final IdempotentConsumer consumer = TestDb.consumer(clock);
    private final Publisher brokerThenConsumer = m -> {
        broker.publish(m);
        consumer.handle(m);
    };
    private PaymentService payments;

    @BeforeEach
    void setUp() {
        TestDb.reset();
        payments = TestDb.payments(clock);
    }

    /** PAYMENTS payments across 5 accounts, each created then captured: 2 outbox rows per payment. */
    private long seed() {
        long expectedCapturedTotal = 0;
        for (int i = 1; i <= PAYMENTS; i++) {
            payments.create("acct-" + (i % 5), 100 + i, "USD");
            payments.capture("pmt-" + i);
            expectedCapturedTotal += 100 + i;
        }
        return expectedCapturedTotal;
    }

    private <T> List<T> runTogether(List<Callable<T>> jobs) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(jobs.size());
        try {
            CyclicBarrier start = new CyclicBarrier(jobs.size());
            List<Future<T>> futures = new ArrayList<>();
            for (Callable<T> job : jobs) {
                futures.add(pool.submit(() -> {
                    start.await(10, TimeUnit.SECONDS);
                    return job.call();
                }));
            }
            List<T> out = new ArrayList<>();
            for (Future<T> f : futures) {
                out.add(f.get(60, TimeUnit.SECONDS));
            }
            return out;
        } finally {
            pool.shutdownNow();
        }
    }

    private Callable<Integer> relayLoop(String id, int batch, long maxAttempts, Publisher target) {
        OutboxRelay relay = new OutboxRelay(repo, target, clock, new RelayConfig(id, batch, 30_000, (int) maxAttempts, 1, 5));
        return () -> {
            int polls = 0;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(45);
            while (repo.countUnpublished() > 0 && System.nanoTime() < deadline) {
                if (relay.pollOnce().claimed() == 0) {
                    Thread.sleep(1);
                }
                polls++;
            }
            return polls;
        };
    }

    private void assertEffectivelyOnce(long expectedCapturedTotal) {
        long total = consumer.totals().values().stream().mapToLong(Long::longValue).sum();
        assertThat(total).as("net captured total applied exactly once").isEqualTo(expectedCapturedTotal);
        assertThat(consumer.eventsApplied()).isEqualTo(PAYMENTS);
        assertThat(consumer.processedCount()).isEqualTo(2L * PAYMENTS);
    }

    private void assertPerAggregateOrder() {
        Map<String, Long> last = new HashMap<>();
        for (Message m : broker.delivered()) {
            long previous = last.getOrDefault(m.aggregateId(), 0L);
            assertThat(m.aggregateSeq()).as("first delivery of %s must follow its predecessor", m.idempotencyKey())
                    .isLessThanOrEqualTo(previous + 1);
            last.merge(m.aggregateId(), m.aggregateSeq(), Math::max);
        }
    }

    @Test
    void twoRelaysRacingPublishEveryRowAndTheConsumerAppliesEachOnce() throws Exception {
        long expected = seed();
        runTogether(List.of(relayLoop("race-a", 7, 5, brokerThenConsumer), relayLoop("race-b", 7, 5, brokerThenConsumer)));

        Set<String> keys = new HashSet<>(broker.deliveredKeys());
        assertThat(keys).as("every row reached the broker at least once").hasSize(2 * PAYMENTS);
        assertThat(repo.findByStatus(Status.PUBLISHED, 1000)).hasSize(2 * PAYMENTS);
        assertThat(repo.countUnpublished()).isZero();
        // With no crash in this run the claim protocol should also have prevented duplicates. That is the
        // property H2 gives us here; it is NOT promised by the at-least-once contract.
        assertThat(broker.delivered()).as("no duplicate deliveries without a crash").hasSize(2 * PAYMENTS);
        assertPerAggregateOrder();
        assertEffectivelyOnce(expected);
    }

    @Test
    void threeRelaysAndABrokerThatFailsEveryThirdCallStillDeliverEverythingInOrder() throws Exception {
        long expected = seed();
        AtomicInteger calls = new AtomicInteger();
        Publisher flaky = m -> {
            if (calls.incrementAndGet() % 3 == 0) {
                throw new PublishException("flaky broker");
            }
            brokerThenConsumer.publish(m);
        };
        runTogether(List.of(relayLoop("f-a", 4, 100, flaky), relayLoop("f-b", 4, 100, flaky),
                relayLoop("f-c", 4, 100, flaky)));

        assertThat(repo.countUnpublished()).isZero();
        assertThat(repo.findByStatus(Status.DEAD, 10)).isEmpty();
        assertThat(new HashSet<>(broker.deliveredKeys())).hasSize(2 * PAYMENTS);
        assertPerAggregateOrder();
        assertEffectivelyOnce(expected);
    }

    @Test
    void aRelayThatDiesAfterPublishingLeavesADuplicateThatTheConsumerDedupes() throws Exception {
        long expected = seed();
        // phase 1: a relay with a short lease publishes ONE row, then "crashes" before marking it
        OutboxRelay doomed = new OutboxRelay(repo, brokerThenConsumer, clock,
                new RelayConfig("doomed", 5, 150, 5, 1, 5), row -> {
                    throw new IllegalStateException("simulated crash");
                });
        try {
            doomed.pollOnce();
        } catch (IllegalStateException expectedCrash) {
            // the thread of the doomed relay is gone; its 5 claimed rows stay CLAIMED until the lease ends
        }
        assertThat(broker.delivered()).hasSize(1);
        assertThat(repo.metrics(clock.millis()).inFlight()).isEqualTo(5);
        // phase 2: two healthy relays take over once the 150 ms lease has really expired
        Thread.sleep(200);
        runTogether(List.of(relayLoop("heal-a", 5, 5, brokerThenConsumer), relayLoop("heal-b", 5, 5, brokerThenConsumer)));

        assertThat(repo.countUnpublished()).isZero();
        Map<String, Integer> perKey = new HashMap<>();
        broker.deliveredKeys().forEach(k -> perKey.merge(k, 1, Integer::sum));
        assertThat(perKey).hasSize(2 * PAYMENTS);
        assertThat(perKey.values().stream().filter(c -> c == 2).count())
                .as("exactly the row that was published before the crash was delivered twice").isEqualTo(1);
        assertThat(broker.delivered()).hasSize(2 * PAYMENTS + 1);
        assertEffectivelyOnce(expected);
    }

    @Test
    void manyThreadsDeliveringTheSameMessageApplyItExactlyOnce() throws Exception {
        Message m = new Message(1, "pmt-x", 2, "PaymentCaptured", "{\"accountId\":\"acct-z\",\"amountMinor\":250}", "pmt-x:2", 1);
        List<Callable<Boolean>> jobs = new ArrayList<>();
        for (int i = 0; i < 12; i++) {
            jobs.add(() -> consumer.handle(m));
        }
        List<Boolean> results = runTogether(jobs);
        assertThat(results.stream().filter(b -> b).count()).isEqualTo(1);
        assertThat(consumer.totals()).containsEntry("acct-z", 250L);
        assertThat(consumer.eventsApplied()).isEqualTo(1);
    }

    @Test
    void concurrentCapturesOfOnePaymentLetExactlyOneWinAndEmitExactlyOneEvent() throws Exception {
        payments.create("acct-1", 100, "USD");
        List<Callable<String>> jobs = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            jobs.add(() -> {
                try {
                    payments.capture("pmt-1");
                    return "ok";
                } catch (ConflictException e) {
                    return "conflict";
                }
            });
        }
        List<String> results = runTogether(jobs);
        assertThat(results.stream().filter("ok"::equals).count()).isEqualTo(1);
        assertThat(results.stream().filter("conflict"::equals).count()).isEqualTo(7);
        List<Row> rows = repo.findByAggregate("pmt-1");
        assertThat(rows).extracting(Row::aggregateSeq).containsExactly(1L, 2L);
    }
}
