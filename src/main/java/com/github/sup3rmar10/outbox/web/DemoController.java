package com.github.sup3rmar10.outbox.web;

import com.github.sup3rmar10.outbox.consumer.IdempotentConsumer;
import com.github.sup3rmar10.outbox.error.NotFoundException;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Message;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Publisher;
import com.github.sup3rmar10.outbox.publish.InMemoryBroker;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Demo-only: look inside the in-memory broker, make it fail, and see what the sample consumer has applied. */
@RestController
public class DemoController {

    private final Publisher broker;
    private final IdempotentConsumer consumer;

    public DemoController(Publisher broker, IdempotentConsumer consumer) {
        this.broker = broker;
        this.consumer = consumer;
    }

    @GetMapping("/demo/broker/messages")
    List<Message> messages() {
        return memory().delivered();
    }

    @PostMapping("/demo/broker/fail")
    Map<String, Integer> fail(@RequestParam int count) {
        if (count < 0 || count > 1000) {
            throw new IllegalArgumentException("count must be 0..1000");
        }
        memory().failNext(count);
        return Map.of("pendingFailures", memory().pendingFailures());
    }

    @GetMapping("/demo/consumer")
    Map<String, Object> consumerState() {
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("processedMessages", consumer.processedCount());
        out.put("eventsApplied", consumer.eventsApplied());
        out.put("netCapturedMinorByAccount", consumer.totals());
        return out;
    }

    private InMemoryBroker memory() {
        if (broker instanceof InMemoryBroker m) {
            return m;
        }
        throw new NotFoundException("the in-memory broker is not active (outbox.publisher is not 'memory')");
    }
}
