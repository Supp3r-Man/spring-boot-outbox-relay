package com.github.sup3rmar10.outbox.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sup3rmar10.outbox.consumer.IdempotentConsumer;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Publisher;
import com.github.sup3rmar10.outbox.outbox.OutboxRelay;
import com.github.sup3rmar10.outbox.outbox.OutboxRepository;
import com.github.sup3rmar10.outbox.outbox.RelayConfig;
import com.github.sup3rmar10.outbox.payment.PaymentService;
import com.github.sup3rmar10.outbox.publish.FileBroker;
import com.github.sup3rmar10.outbox.publish.InMemoryBroker;
import java.nio.file.Path;
import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
public class AppConfig {

    @Bean
    Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    OutboxRepository outboxRepository(JdbcTemplate jdbc) {
        return new OutboxRepository(jdbc);
    }

    @Bean
    PaymentService paymentService(JdbcTemplate jdbc, OutboxRepository outbox, PlatformTransactionManager tm,
                                  Clock clock, ObjectMapper json) {
        return new PaymentService(jdbc, outbox, tm, clock, json);
    }

    @Bean
    IdempotentConsumer idempotentConsumer(JdbcTemplate jdbc, PlatformTransactionManager tm, Clock clock,
                                          ObjectMapper json) {
        return new IdempotentConsumer(jdbc, tm, clock, json);
    }

    /** The broker the relay publishes to: "memory" (default) or "file". Both are stand-ins, not Kafka. */
    @Bean
    Publisher broker(OutboxProperties props, ObjectMapper json) {
        return switch (props.publisher()) {
            case "memory" -> new InMemoryBroker();
            case "file" -> new FileBroker(Path.of(props.filePath()), json);
            default -> throw new IllegalArgumentException("outbox.publisher must be memory or file, got " + props.publisher());
        };
    }

    @Bean
    OutboxRelay outboxRelay(OutboxRepository repo, Publisher broker, IdempotentConsumer consumer, Clock clock,
                            OutboxProperties props) {
        Publisher target = props.demoConsumer()
                ? message -> {
                    broker.publish(message);
                    consumer.handle(message);
                }
                : broker;
        RelayConfig config = new RelayConfig(props.relayId(), props.batchSize(), props.leaseMillis(),
                props.maxAttempts(), props.baseBackoffMillis(), props.maxBackoffMillis());
        return new OutboxRelay(repo, target, clock, config);
    }
}
