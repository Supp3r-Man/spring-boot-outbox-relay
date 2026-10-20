package com.github.sup3rmar10.outbox.publish;

import com.github.sup3rmar10.outbox.outbox.OutboxModel.Message;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.PublishException;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Publisher;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/** Thread-safe test broker that records every delivery (duplicates included) and can be told to fail. */
public class InMemoryBroker implements Publisher {

    private final List<Message> delivered = new ArrayList<>();
    private int failNext;
    private Predicate<Message> failWhen = m -> false;

    @Override
    public synchronized void publish(Message message) throws PublishException {
        if (failNext > 0) {
            failNext--;
            throw new PublishException("broker unavailable (injected)");
        }
        if (failWhen.test(message)) {
            throw new PublishException("broker rejected " + message.idempotencyKey() + " (injected)");
        }
        delivered.add(message);
    }

    /** The next n publish calls fail, whatever the message. */
    public synchronized void failNext(int n) {
        failNext = n;
    }

    /** Publish calls fail while the predicate matches (pass null to clear). */
    public synchronized void failWhen(Predicate<Message> predicate) {
        failWhen = predicate == null ? m -> false : predicate;
    }

    public synchronized List<Message> delivered() {
        return List.copyOf(delivered);
    }

    public synchronized List<String> deliveredKeys() {
        return delivered.stream().map(Message::idempotencyKey).toList();
    }

    public synchronized int pendingFailures() {
        return failNext;
    }
}
