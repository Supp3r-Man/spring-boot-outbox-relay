package com.github.sup3rmar10.outbox.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.github.sup3rmar10.outbox.outbox.OutboxModel.Metrics;
import com.github.sup3rmar10.outbox.outbox.OutboxRepository;
import com.github.sup3rmar10.outbox.payment.Payment;
import com.github.sup3rmar10.outbox.payment.PaymentService;
import java.time.Clock;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/** The background scheduler really runs the relay: nobody calls pollOnce here. */
@SpringBootTest(properties = {
        "outbox.scheduler-enabled=true",
        "outbox.poll-interval-millis=50",
        "spring.datasource.url=jdbc:h2:mem:schedtest;DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000"})
class SchedulerTest {

    @Autowired PaymentService payments;
    @Autowired OutboxRepository outbox;
    @Autowired Clock clock;

    @Test
    void theScheduledRelayPublishesRowsWithoutAnyExplicitPoll() {
        Payment p = payments.create("acct-1", 100, "USD");
        payments.capture(p.id());
        await().atMost(Duration.ofSeconds(15)).pollInterval(Duration.ofMillis(50)).untilAsserted(() -> {
            Metrics m = outbox.metrics(clock.millis());
            assertThat(m.published()).isEqualTo(2);
            assertThat(m.pending()).isZero();
        });
    }
}
