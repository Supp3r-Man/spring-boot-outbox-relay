package com.github.sup3rmar10.outbox.consumer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.sup3rmar10.outbox.MutableClock;
import com.github.sup3rmar10.outbox.TestDb;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Message;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class IdempotentConsumerTest {

    private final IdempotentConsumer consumer = TestDb.consumer(new MutableClock(1));

    @BeforeEach
    void clean() {
        TestDb.reset();
    }

    private static Message msg(String key, String type, String account, long amount) {
        return new Message(1, "pmt", 1, type, "{\"accountId\":\"" + account + "\",\"amountMinor\":" + amount + "}", key, 1);
    }

    @Test
    void aCapturedEventAddsToTheAccountTotal() {
        assertThat(consumer.handle(msg("k1", "PaymentCaptured", "acct-1", 500))).isTrue();
        assertThat(consumer.totals()).isEqualTo(Map.of("acct-1", 500L));
        assertThat(consumer.processedCount()).isEqualTo(1);
    }

    @Test
    void redeliveringTheSameKeyDoesNothing() {
        Message m = msg("k1", "PaymentCaptured", "acct-1", 500);
        assertThat(consumer.handle(m)).isTrue();
        assertThat(consumer.handle(m)).isFalse();
        assertThat(consumer.handle(m)).isFalse();
        assertThat(consumer.totals()).isEqualTo(Map.of("acct-1", 500L));
        assertThat(consumer.eventsApplied()).isEqualTo(1);
        assertThat(consumer.processedCount()).isEqualTo(1);
    }

    @Test
    void duplicatesAreRecognisedByKeyNotByContent() {
        consumer.handle(msg("k1", "PaymentCaptured", "acct-1", 500));
        assertThat(consumer.handle(msg("k2", "PaymentCaptured", "acct-1", 500))).isTrue();
        assertThat(consumer.totals()).isEqualTo(Map.of("acct-1", 1000L));
        // same key but different content is still a duplicate: the key is the identity of the event
        assertThat(consumer.handle(msg("k2", "PaymentCaptured", "acct-1", 999))).isFalse();
        assertThat(consumer.totals()).isEqualTo(Map.of("acct-1", 1000L));
    }

    @Test
    void aRefundSubtractsAndAccountsAreIndependent() {
        consumer.handle(msg("k1", "PaymentCaptured", "acct-1", 500));
        consumer.handle(msg("k2", "PaymentCaptured", "acct-2", 300));
        consumer.handle(msg("k3", "PaymentRefunded", "acct-1", 200));
        assertThat(consumer.totals()).isEqualTo(Map.of("acct-1", 300L, "acct-2", 300L));
        assertThat(consumer.eventsApplied()).isEqualTo(3);
    }

    @Test
    void otherEventTypesAreRecordedAsProcessedButHaveNoEffect() {
        assertThat(consumer.handle(msg("k1", "PaymentCreated", "acct-1", 500))).isTrue();
        assertThat(consumer.totals()).isEmpty();
        assertThat(consumer.processedCount()).isEqualTo(1);
        assertThat(consumer.handle(msg("k1", "PaymentCreated", "acct-1", 500))).isFalse();
    }

    @Test
    void aFailingEffectRollsBackTheKeySoALaterRetryCanSucceed() {
        Message bad = new Message(1, "pmt", 2, "PaymentCaptured", "not json", "k9", 1);
        assertThatThrownBy(() -> consumer.handle(bad)).isInstanceOf(IllegalArgumentException.class);
        assertThat(consumer.processedCount()).as("key must not be recorded when the effect failed").isZero();
        Message good = msg("k9", "PaymentCaptured", "acct-1", 70);
        assertThat(consumer.handle(good)).isTrue();
        assertThat(consumer.totals()).isEqualTo(Map.of("acct-1", 70L));
    }
}
