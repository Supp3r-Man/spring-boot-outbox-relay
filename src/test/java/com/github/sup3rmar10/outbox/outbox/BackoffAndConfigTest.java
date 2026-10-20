package com.github.sup3rmar10.outbox.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class BackoffAndConfigTest {

    @Test
    void firstFailureWaitsTheBaseDelay() {
        assertThat(Backoff.delayMillis(1, 1000, 60_000)).isEqualTo(1000);
    }

    @Test
    void delayDoublesWithEachAttempt() {
        assertThat(Backoff.delayMillis(2, 1000, 60_000)).isEqualTo(2000);
        assertThat(Backoff.delayMillis(3, 1000, 60_000)).isEqualTo(4000);
        assertThat(Backoff.delayMillis(4, 1000, 60_000)).isEqualTo(8000);
        assertThat(Backoff.delayMillis(6, 1000, 60_000)).isEqualTo(32_000);
    }

    @Test
    void delayIsCappedAtMax() {
        assertThat(Backoff.delayMillis(7, 1000, 60_000)).isEqualTo(60_000);
        assertThat(Backoff.delayMillis(20, 1000, 60_000)).isEqualTo(60_000);
    }

    @Test
    void hugeAttemptCountsDoNotOverflow() {
        assertThat(Backoff.delayMillis(Integer.MAX_VALUE, 1000, Long.MAX_VALUE)).isEqualTo(1000L << 30);
        assertThat(Backoff.delayMillis(100, 1_000_000_000_000L, Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
    }

    @Test
    void zeroBaseMeansImmediateRetry() {
        assertThat(Backoff.delayMillis(5, 0, 0)).isZero();
    }

    @Test
    void nonPositiveAttemptsAreTreatedAsTheFirst() {
        assertThat(Backoff.delayMillis(0, 500, 10_000)).isEqualTo(500);
        assertThat(Backoff.delayMillis(-3, 500, 10_000)).isEqualTo(500);
    }

    @Test
    void validConfigIsAccepted() {
        RelayConfig c = new RelayConfig("relay-a", 5, 1000, 3, 100, 100);
        assertThat(c.relayId()).isEqualTo("relay-a");
        assertThat(c.maxAttempts()).isEqualTo(3);
    }

    @Test
    void invalidConfigIsRejected() {
        assertThatThrownBy(() -> new RelayConfig(" ", 5, 1000, 3, 100, 100)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RelayConfig("x".repeat(41), 5, 1000, 3, 100, 100))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RelayConfig("r", 0, 1000, 3, 100, 100)).hasMessageContaining("batchSize");
        assertThatThrownBy(() -> new RelayConfig("r", 5, 0, 3, 100, 100)).hasMessageContaining("leaseMillis");
        assertThatThrownBy(() -> new RelayConfig("r", 5, 1000, 0, 100, 100)).hasMessageContaining("maxAttempts");
        assertThatThrownBy(() -> new RelayConfig("r", 5, 1000, 3, 200, 100)).hasMessageContaining("Backoff");
        assertThatThrownBy(() -> new RelayConfig("r", 5, 1000, 3, -1, 100)).hasMessageContaining("Backoff");
    }
}
