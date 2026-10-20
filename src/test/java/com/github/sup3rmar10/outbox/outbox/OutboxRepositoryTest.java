package com.github.sup3rmar10.outbox.outbox;

import static com.github.sup3rmar10.outbox.TestDb.event;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.github.sup3rmar10.outbox.TestDb;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Metrics;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.NewEvent;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Row;
import com.github.sup3rmar10.outbox.outbox.OutboxModel.Status;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DuplicateKeyException;

/** The claim protocol, one statement at a time, with explicit "now" values (no clock, no threads). */
class OutboxRepositoryTest {

    private final OutboxRepository repo = TestDb.repo();

    @BeforeEach
    void clean() {
        TestDb.reset();
    }

    private long ins(String agg, long seq) {
        return repo.insert(event(agg, seq), 1000);
    }

    private List<Row> claim(String token, long now, int limit) {
        return repo.claim(token, now, now + 100, limit);
    }

    @Test
    void insertStoresANewRowWithDefaults() {
        long id = ins("a", 1);
        Row r = repo.findById(id);
        assertThat(r.status()).isEqualTo(Status.NEW);
        assertThat(r.attempts()).isZero();
        assertThat(r.nextAttemptAt()).isEqualTo(1000);
        assertThat(r.createdAt()).isEqualTo(1000);
        assertThat(r.idempotencyKey()).isEqualTo("a:1");
        assertThat(r.leaseOwner()).isNull();
        assertThat(r.publishedAt()).isNull();
    }

    @Test
    void duplicateIdempotencyKeyIsRejected() {
        ins("a", 1);
        NewEvent clash = new NewEvent("b", 1, "X", "{}", "a:1");
        assertThatThrownBy(() -> repo.insert(clash, 1000)).isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    void duplicateAggregateSequenceIsRejected() {
        ins("a", 1);
        NewEvent clash = new NewEvent("a", 1, "X", "{}", "other-key");
        assertThatThrownBy(() -> repo.insert(clash, 1000)).isInstanceOf(DuplicateKeyException.class);
    }

    @Test
    void claimOnAnEmptyTableReturnsNothing() {
        assertThat(claim("t1", 1000, 10)).isEmpty();
    }

    @Test
    void claimSetsLeaseOwnerAndCountsTheAttempt() {
        ins("a", 1);
        List<Row> rows = repo.claim("t1", 1000, 1100, 10);
        assertThat(rows).hasSize(1);
        Row r = rows.get(0);
        assertThat(r.status()).isEqualTo(Status.CLAIMED);
        assertThat(r.leaseOwner()).isEqualTo("t1");
        assertThat(r.leaseUntil()).isEqualTo(1100);
        assertThat(r.attempts()).isEqualTo(1);
    }

    @Test
    void rowsNotYetDueAreNotClaimedButDueIsInclusive() {
        repo.insert(event("a", 1), 5000);
        assertThat(claim("t1", 4999, 10)).isEmpty();
        assertThat(claim("t2", 5000, 10)).hasSize(1);
    }

    @Test
    void claimRespectsTheBatchSizeAndTakesTheOldestFirst() {
        long first = ins("a", 1);
        long second = ins("b", 1);
        ins("c", 1);
        ins("d", 1);
        List<Row> rows = claim("t1", 1000, 2);
        assertThat(rows).extracting(Row::id).containsExactly(first, second);
        assertThat(claim("t2", 1000, 10)).hasSize(2);
    }

    @Test
    void claimTakesTheLowestIdFirstEvenWhenAHigherIdBecameDueEarlier() {
        // the (status, next_attempt_at) index would hand back "b" first if the claim did not order by id
        long first = repo.insert(event("a", 1), 3000);
        repo.insert(event("b", 1), 1000);
        assertThat(claim("t1", 5000, 1)).extracting(Row::id).containsExactly(first);
    }

    @Test
    void aClaimedRowIsNotOfferedToAnotherRelayBeforeTheLeaseExpires() {
        ins("a", 1);
        repo.claim("t1", 1000, 1100, 10);
        assertThat(repo.claim("t2", 1099, 1200, 10)).isEmpty();
    }

    @Test
    void anExpiredLeaseCanBeTakenOverAndTheLeaseEndIsInclusive() {
        long id = ins("a", 1);
        repo.claim("t1", 1000, 1100, 10);
        List<Row> rows = repo.claim("t2", 1100, 1200, 10);
        assertThat(rows).hasSize(1);
        assertThat(rows.get(0).leaseOwner()).isEqualTo("t2");
        assertThat(rows.get(0).attempts()).isEqualTo(2);
        // the old owner is fenced out
        assertThat(repo.markPublished(id, "t1", 1150)).isFalse();
        assertThat(repo.markPublished(id, "t2", 1150)).isTrue();
    }

    @Test
    void onlyTheOldestUnfinishedEventOfEachAggregateIsOffered() {
        ins("a", 1);
        ins("a", 2);
        ins("a", 3);
        ins("b", 1);
        List<Row> rows = claim("t1", 1000, 10);
        assertThat(rows).extracting(Row::idempotencyKey).containsExactly("a:1", "b:1");
    }

    @Test
    void laterEventsWaitWhileTheEarlierOneIsClaimedByAnotherRelay() {
        ins("a", 1);
        ins("a", 2);
        claim("t1", 1000, 10);
        assertThat(claim("t2", 1050, 10)).isEmpty();
    }

    @Test
    void laterEventsWaitWhileTheEarlierOneIsInBackoff() {
        long first = ins("a", 1);
        ins("a", 2);
        claim("t1", 1000, 10);
        repo.markRetry(first, "t1", 9000, "boom");
        assertThat(claim("t2", 1000, 10)).isEmpty();
        assertThat(claim("t3", 9000, 10)).extracting(Row::idempotencyKey).containsExactly("a:1");
    }

    @Test
    void aDeadEventBlocksLaterEventsOfTheSameAggregateOnly() {
        long first = ins("a", 1);
        ins("a", 2);
        ins("b", 1);
        claim("t1", 1000, 1);
        repo.markDead(first, "t1", "poison");
        List<Row> rows = claim("t2", 2000, 10);
        assertThat(rows).extracting(Row::idempotencyKey).containsExactly("b:1");
    }

    @Test
    void theNextEventIsReleasedOncetheEarlierOneIsPublished() {
        long first = ins("a", 1);
        ins("a", 2);
        claim("t1", 1000, 10);
        repo.markPublished(first, "t1", 1010);
        assertThat(claim("t2", 1010, 10)).extracting(Row::idempotencyKey).containsExactly("a:2");
    }

    @Test
    void markPublishedRecordsTimeAndClearsTheLease() {
        long id = ins("a", 1);
        claim("t1", 1000, 10);
        assertThat(repo.markPublished(id, "t1", 1234)).isTrue();
        Row r = repo.findById(id);
        assertThat(r.status()).isEqualTo(Status.PUBLISHED);
        assertThat(r.publishedAt()).isEqualTo(1234);
        assertThat(r.leaseOwner()).isNull();
        assertThat(r.leaseUntil()).isNull();
        assertThat(repo.markPublished(id, "t1", 1300)).as("second mark is a no-op").isFalse();
        assertThat(claim("t2", 99_999, 10)).isEmpty();
    }

    @Test
    void finalStateWritesAreFencedByTheClaimToken() {
        long id = ins("a", 1);
        claim("t1", 1000, 10);
        assertThat(repo.markPublished(id, "intruder", 1)).isFalse();
        assertThat(repo.markRetry(id, "intruder", 5000, "x")).isFalse();
        assertThat(repo.markDead(id, "intruder", "x")).isFalse();
        Row r = repo.findById(id);
        assertThat(r.status()).isEqualTo(Status.CLAIMED);
        assertThat(r.leaseOwner()).isEqualTo("t1");
    }

    @Test
    void markRetryReturnsTheRowToNewWithTheScheduleAndError() {
        long id = ins("a", 1);
        claim("t1", 1000, 10);
        assertThat(repo.markRetry(id, "t1", 4000, "broker down")).isTrue();
        Row r = repo.findById(id);
        assertThat(r.status()).isEqualTo(Status.NEW);
        assertThat(r.nextAttemptAt()).isEqualTo(4000);
        assertThat(r.lastError()).isEqualTo("broker down");
        assertThat(r.leaseOwner()).isNull();
        assertThat(r.attempts()).isEqualTo(1);
    }

    @Test
    void markDeadSetsTheDeadStateAndKeepsTheError() {
        long id = ins("a", 1);
        claim("t1", 1000, 10);
        assertThat(repo.markDead(id, "t1", "gave up")).isTrue();
        Row r = repo.findById(id);
        assertThat(r.status()).isEqualTo(Status.DEAD);
        assertThat(r.lastError()).isEqualTo("gave up");
        assertThat(r.leaseOwner()).isNull();
    }

    @Test
    void requeueAllDeadMovesOnlyDeadRowsAndResetsTheAttemptBudget() {
        long dead1 = ins("a", 1);
        long dead2 = ins("b", 1);
        long live = ins("c", 1);
        claim("t1", 1000, 2);
        repo.markDead(dead1, "t1", "x");
        repo.markDead(dead2, "t1", "y");
        assertThat(repo.requeueAllDead(7000)).isEqualTo(2);
        Row r = repo.findById(dead1);
        assertThat(r.status()).isEqualTo(Status.NEW);
        assertThat(r.attempts()).isZero();
        assertThat(r.nextAttemptAt()).isEqualTo(7000);
        assertThat(repo.findById(live).status()).isEqualTo(Status.NEW);
        assertThat(repo.requeueAllDead(8000)).isZero();
    }

    @Test
    void requeueOneTouchesOnlyThatDeadRow() {
        long a = ins("a", 1);
        long b = ins("b", 1);
        claim("t1", 1000, 2);
        repo.markDead(a, "t1", "x");
        repo.markDead(b, "t1", "y");
        assertThat(repo.requeueDead(a, 7000)).isEqualTo(1);
        assertThat(repo.findById(a).status()).isEqualTo(Status.NEW);
        assertThat(repo.findById(b).status()).isEqualTo(Status.DEAD);
        assertThat(repo.requeueDead(a, 7000)).as("no longer dead").isZero();
    }

    @Test
    void metricsCountEachStateAndReportTheOldestUnpublishedAge() {
        long pub = repo.insert(event("p", 1), 500);
        long dead = repo.insert(event("d", 1), 600);
        repo.insert(event("n", 1), 700);
        repo.insert(event("c", 1), 800);
        repo.claim("t1", 1000, 1100, 2); // p and d
        repo.markPublished(pub, "t1", 1000);
        repo.markDead(dead, "t1", "x");
        repo.claim("t2", 1000, 1100, 1); // n -> CLAIMED (oldest remaining NEW row)
        Metrics m = repo.metrics(1500);
        assertThat(m.published()).isEqualTo(1);
        assertThat(m.dead()).isEqualTo(1);
        assertThat(m.inFlight()).isEqualTo(1);
        assertThat(m.pending()).isEqualTo(1);
        assertThat(m.oldestUnpublishedAgeMillis()).isEqualTo(800); // the unpublished rows were created at 700 and 800
    }

    @Test
    void metricsOnAnEmptyOrFullyPublishedOutboxHaveNoLag() {
        assertThat(repo.metrics(10)).isEqualTo(new Metrics(0, 0, 0, 0, null));
        long id = ins("a", 1);
        claim("t1", 1000, 1);
        repo.markPublished(id, "t1", 1001);
        assertThat(repo.metrics(5000)).isEqualTo(new Metrics(0, 0, 1, 0, null));
    }

    @Test
    void findByStatusFiltersOrdersAndLimits() {
        long a = ins("a", 1);
        long b = ins("b", 1);
        ins("c", 1);
        claim("t1", 1000, 2);
        repo.markDead(a, "t1", "x");
        repo.markDead(b, "t1", "y");
        assertThat(repo.findByStatus(Status.DEAD, 10)).extracting(Row::id).containsExactly(a, b);
        assertThat(repo.findByStatus(Status.DEAD, 1)).extracting(Row::id).containsExactly(a);
        assertThat(repo.findByStatus(Status.NEW, 10)).hasSize(1);
    }

    @Test
    void countUnpublishedCountsEverythingExceptPublished() {
        long a = ins("a", 1);
        ins("b", 1);
        claim("t1", 1000, 1);
        repo.markPublished(a, "t1", 1001);
        assertThat(repo.countUnpublished()).isEqualTo(1);
    }
}
