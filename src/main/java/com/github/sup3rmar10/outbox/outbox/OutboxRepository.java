package com.github.sup3rmar10.outbox.outbox;

import com.github.sup3rmar10.outbox.outbox.OutboxModel.Metrics;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.NewEvent;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Row;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Status;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

/**
 * All outbox SQL. insert() joins the caller's transaction; every other method is a single autocommit statement
 * (or joins a transaction if the caller has one).
 *
 * Claiming is a compare-and-set UPDATE, not SELECT ... FOR UPDATE SKIP LOCKED: the sub-select picks candidates,
 * and the outer WHERE repeats the eligibility test so a row that another relay claimed in the meantime is
 * skipped by the loser. See the README for what H2 does and does not prove about that.
 */
public class OutboxRepository {

    private static final RowMapper<Row> MAPPER = (rs, i) -> map(rs);

    private static final String CLAIM_SQL = """
            UPDATE outbox SET status = 'CLAIMED', lease_owner = ?, lease_until = ?, attempts = attempts + 1
            WHERE id IN (
              SELECT o.id FROM outbox o
              WHERE ((o.status = 'NEW' AND o.next_attempt_at <= ?) OR (o.status = 'CLAIMED' AND o.lease_until <= ?))
                AND NOT EXISTS (SELECT 1 FROM outbox p
                                WHERE p.aggregate_id = o.aggregate_id
                                  AND p.aggregate_seq < o.aggregate_seq
                                  AND p.status <> 'PUBLISHED')
              ORDER BY o.id
              LIMIT ?)
            AND ((status = 'NEW' AND next_attempt_at <= ?) OR (status = 'CLAIMED' AND lease_until <= ?))
            """;

    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public long insert(NewEvent e, long now) {
        KeyHolder keys = new GeneratedKeyHolder();
        jdbc.update(con -> {
            PreparedStatement ps = con.prepareStatement(
                    "INSERT INTO outbox (aggregate_id, aggregate_seq, event_type, payload, idempotency_key, status,"
                            + " attempts, next_attempt_at, created_at) VALUES (?, ?, ?, ?, ?, 'NEW', 0, ?, ?)",
                    Statement.RETURN_GENERATED_KEYS);
            ps.setString(1, e.aggregateId());
            ps.setLong(2, e.aggregateSeq());
            ps.setString(3, e.eventType());
            ps.setString(4, e.payload());
            ps.setString(5, e.idempotencyKey());
            ps.setLong(6, now);
            ps.setLong(7, now);
            return ps;
        }, keys);
        return keys.getKey().longValue();
    }

    /** Claims up to limit due rows for one relay. token must be unique per call (relay id + random suffix). */
    public List<Row> claim(String token, long now, long leaseUntil, int limit) {
        int n = jdbc.update(CLAIM_SQL, token, leaseUntil, now, now, limit, now, now);
        if (n == 0) {
            return List.of();
        }
        return jdbc.query("SELECT * FROM outbox WHERE status = 'CLAIMED' AND lease_owner = ? ORDER BY id", MAPPER, token);
    }

    public boolean markPublished(long id, String token, long now) {
        return jdbc.update("UPDATE outbox SET status = 'PUBLISHED', published_at = ?, lease_owner = NULL,"
                + " lease_until = NULL WHERE id = ? AND status = 'CLAIMED' AND lease_owner = ?", now, id, token) == 1;
    }

    public boolean markRetry(long id, String token, long nextAttemptAt, String error) {
        return jdbc.update("UPDATE outbox SET status = 'NEW', next_attempt_at = ?, last_error = ?, lease_owner = NULL,"
                + " lease_until = NULL WHERE id = ? AND status = 'CLAIMED' AND lease_owner = ?",
                nextAttemptAt, error, id, token) == 1;
    }

    public boolean markDead(long id, String token, String error) {
        return jdbc.update("UPDATE outbox SET status = 'DEAD', last_error = ?, lease_owner = NULL,"
                + " lease_until = NULL WHERE id = ? AND status = 'CLAIMED' AND lease_owner = ?", error, id, token) == 1;
    }

    /** Moves DEAD rows back to NEW with a fresh attempt budget. Returns how many rows moved. */
    public int requeueAllDead(long now) {
        return jdbc.update("UPDATE outbox SET status = 'NEW', attempts = 0, next_attempt_at = ?"
                + " WHERE status = 'DEAD'", now);
    }

    public int requeueDead(long id, long now) {
        return jdbc.update("UPDATE outbox SET status = 'NEW', attempts = 0, next_attempt_at = ?"
                + " WHERE status = 'DEAD' AND id = ?", now, id);
    }

    public Row findById(long id) {
        List<Row> rows = jdbc.query("SELECT * FROM outbox WHERE id = ?", MAPPER, id);
        return rows.isEmpty() ? null : rows.get(0);
    }

    public List<Row> findByStatus(Status status, int limit) {
        return jdbc.query("SELECT * FROM outbox WHERE status = ? ORDER BY id LIMIT ?", MAPPER, status.name(), limit);
    }

    public List<Row> findByAggregate(String aggregateId) {
        return jdbc.query("SELECT * FROM outbox WHERE aggregate_id = ? ORDER BY aggregate_seq", MAPPER, aggregateId);
    }

    public long countUnpublished() {
        Long n = jdbc.queryForObject("SELECT COUNT(*) FROM outbox WHERE status <> 'PUBLISHED'", Long.class);
        return n == null ? 0 : n;
    }

    public Metrics metrics(long now) {
        Map<Status, Long> counts = new EnumMap<>(Status.class);
        jdbc.query("SELECT status, COUNT(*) AS c FROM outbox GROUP BY status",
                rs -> { counts.put(Status.valueOf(rs.getString("status")), rs.getLong("c")); });
        Long oldest = jdbc.queryForObject(
                "SELECT MIN(created_at) FROM outbox WHERE status IN ('NEW', 'CLAIMED')", Long.class);
        return new Metrics(
                counts.getOrDefault(Status.NEW, 0L),
                counts.getOrDefault(Status.CLAIMED, 0L),
                counts.getOrDefault(Status.PUBLISHED, 0L),
                counts.getOrDefault(Status.DEAD, 0L),
                oldest == null ? null : Math.max(0, now - oldest));
    }

    private static Row map(ResultSet rs) throws SQLException {
        return new Row(
                rs.getLong("id"),
                rs.getString("aggregate_id"),
                rs.getLong("aggregate_seq"),
                rs.getString("event_type"),
                rs.getString("payload"),
                rs.getString("idempotency_key"),
                Status.valueOf(rs.getString("status")),
                rs.getInt("attempts"),
                rs.getLong("next_attempt_at"),
                rs.getString("lease_owner"),
                rs.getObject("lease_until", Long.class),
                rs.getString("last_error"),
                rs.getLong("created_at"),
                rs.getObject("published_at", Long.class));
    }
}
