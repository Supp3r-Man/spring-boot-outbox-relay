package com.github.sup3rmar10.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.sup3rmar10.outbox.consumer.IdempotentConsumer;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.NewEvent;
import com.github.sup3rmar10.outbox.outbox.OutboxRepository;
import com.github.sup3rmar10.outbox.payment.PaymentService;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.time.Clock;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

/**
 * One in-memory H2 database per JVM, a real connection pool (so threads really get separate connections and
 * transactions), and plain objects instead of a Spring context: this keeps the whole suite fast enough to be
 * re-run for every mutation.
 */
public final class TestDb {
    public static final HikariDataSource DS;
    public static final JdbcTemplate JDBC;
    public static final DataSourceTransactionManager TM;
    public static final ObjectMapper JSON = new ObjectMapper();

    static {
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl("jdbc:h2:mem:plain-" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1;LOCK_TIMEOUT=10000");
        cfg.setMaximumPoolSize(24);
        DS = new HikariDataSource(cfg);
        new ResourceDatabasePopulator(new ClassPathResource("schema.sql")).execute(DS);
        JDBC = new JdbcTemplate(DS);
        TM = new DataSourceTransactionManager(DS);
    }

    private TestDb() {
    }

    public static void reset() {
        JDBC.update("DELETE FROM outbox");
        JDBC.update("DELETE FROM payment");
        JDBC.update("DELETE FROM processed_message");
        JDBC.update("DELETE FROM account_captured");
    }

    public static OutboxRepository repo() {
        return new OutboxRepository(JDBC);
    }

    public static IdempotentConsumer consumer(Clock clock) {
        return new IdempotentConsumer(JDBC, TM, clock, JSON);
    }

    /** Payment ids are pmt-1, pmt-2, ... so assertions can name them. */
    public static PaymentService payments(Clock clock) {
        AtomicInteger n = new AtomicInteger();
        return new PaymentService(JDBC, repo(), TM, clock, JSON, () -> "pmt-" + n.incrementAndGet());
    }

    public static NewEvent event(String aggregate, long seq) {
        return event(aggregate, seq, "PaymentCreated");
    }

    public static NewEvent event(String aggregate, long seq, String type) {
        return new NewEvent(aggregate, seq, type, "{\"paymentId\":\"" + aggregate + "\"}", aggregate + ":" + seq);
    }
}
