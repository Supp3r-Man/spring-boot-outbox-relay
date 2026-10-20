package com.github.sup3rmar10.outbox.publish;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Message;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.PublishException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BrokerTest {

    private static Message msg(String agg, long seq) {
        return new Message(seq, agg, seq, "PaymentCreated", "{\"a\":1}", agg + ":" + seq, 1);
    }

    @Test
    void inMemoryBrokerRecordsDeliveriesInOrderIncludingDuplicates() throws Exception {
        InMemoryBroker b = new InMemoryBroker();
        b.publish(msg("a", 1));
        b.publish(msg("a", 2));
        b.publish(msg("a", 1));
        assertThat(b.deliveredKeys()).containsExactly("a:1", "a:2", "a:1");
    }

    @Test
    void failNextFailsExactlyThatManyCalls() throws Exception {
        InMemoryBroker b = new InMemoryBroker();
        b.failNext(2);
        assertThatThrownBy(() -> b.publish(msg("a", 1))).isInstanceOf(PublishException.class);
        assertThatThrownBy(() -> b.publish(msg("a", 1))).isInstanceOf(PublishException.class);
        b.publish(msg("a", 1));
        assertThat(b.delivered()).hasSize(1);
        assertThat(b.pendingFailures()).isZero();
    }

    @Test
    void failWhenRejectsMatchingMessagesOnlyAndCanBeCleared() throws Exception {
        InMemoryBroker b = new InMemoryBroker();
        b.failWhen(m -> m.aggregateId().equals("bad"));
        b.publish(msg("good", 1));
        assertThatThrownBy(() -> b.publish(msg("bad", 1))).hasMessageContaining("bad:1");
        b.failWhen(null);
        b.publish(msg("bad", 1));
        assertThat(b.deliveredKeys()).containsExactly("good:1", "bad:1");
    }

    @Test
    void fileBrokerAppendsOneJsonLinePerMessageAndReadsThemBack(@TempDir Path dir) throws Exception {
        Path file = dir.resolve("broker.jsonl");
        FileBroker b = new FileBroker(file, new ObjectMapper());
        b.publish(msg("a", 1));
        b.publish(msg("a", 2));
        assertThat(Files.readAllLines(file)).hasSize(2);
        assertThat(b.readAll()).containsExactly(msg("a", 1), msg("a", 2));
        // a second instance on the same file appends instead of truncating
        new FileBroker(file, new ObjectMapper()).publish(msg("b", 1));
        assertThat(b.readAll()).hasSize(3);
    }

    @Test
    void fileBrokerReadsAnEmptyListWhenTheFileDoesNotExist(@TempDir Path dir) throws IOException {
        assertThat(new FileBroker(dir.resolve("none.jsonl"), new ObjectMapper()).readAll()).isEmpty();
    }

    @Test
    void fileBrokerReportsIoFailuresAsPublishExceptions(@TempDir Path dir) {
        // the target is a directory, so appending must fail
        FileBroker b = new FileBroker(dir, new ObjectMapper());
        assertThatThrownBy(() -> b.publish(msg("a", 1))).isInstanceOf(PublishException.class)
                .hasMessageContaining("cannot append");
    }
}
