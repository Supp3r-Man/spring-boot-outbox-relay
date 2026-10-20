package com.github.sup3rmar10.outbox.web;

import com.github.sup3rmar10.outbox.error.ConflictException;
import com.github.sup3rmar10.outbox.error.NotFoundException;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Metrics;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.PollResult;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Row;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Status;
import com.github.sup3rmar10.outbox.outbox.OutboxRelay;
import com.github.sup3rmar10.outbox.outbox.OutboxRepository;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/** Operational endpoints. There is NO authentication on these (see README, known limits). */
@RestController
public class OutboxAdminController {

    private final OutboxRepository outbox;
    private final OutboxRelay relay;
    private final Clock clock;

    public OutboxAdminController(OutboxRepository outbox, OutboxRelay relay, Clock clock) {
        this.outbox = outbox;
        this.relay = relay;
        this.clock = clock;
    }

    @GetMapping("/outbox/metrics")
    Metrics metrics() {
        return outbox.metrics(clock.millis());
    }

    @GetMapping("/admin/outbox")
    List<Row> list(@RequestParam(defaultValue = "DEAD") String status, @RequestParam(defaultValue = "50") int limit) {
        Status parsed;
        try {
            parsed = Status.valueOf(status.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("status must be one of NEW, CLAIMED, PUBLISHED, DEAD");
        }
        if (limit < 1 || limit > 500) {
            throw new IllegalArgumentException("limit must be 1..500");
        }
        return outbox.findByStatus(parsed, limit);
    }

    @PostMapping("/admin/outbox/dead/requeue")
    Map<String, Integer> requeueAllDead() {
        return Map.of("requeued", outbox.requeueAllDead(clock.millis()));
    }

    @PostMapping("/admin/outbox/{id}/requeue")
    Map<String, Integer> requeueOne(@PathVariable long id) {
        int n = outbox.requeueDead(id, clock.millis());
        if (n == 0) {
            Row row = outbox.findById(id);
            if (row == null) {
                throw new NotFoundException("outbox row " + id + " not found");
            }
            throw new ConflictException("outbox row " + id + " is " + row.status() + ", only DEAD rows can be requeued");
        }
        return Map.of("requeued", n);
    }

    /** Runs one relay poll right now (the background scheduler may also be running). */
    @PostMapping("/admin/relay/poll")
    PollResult poll() {
        return relay.pollOnce();
    }
}
