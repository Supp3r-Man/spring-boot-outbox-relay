# spring-boot-outbox-relay

The transactional outbox pattern in Spring Boot 3.4 / Java 17 / Maven, with a polling relay, on H2 and plain
`JdbcTemplate`. A small payments-style service writes a domain change **and** an outbox row in one database
transaction; a relay publishes the outbox rows to a pluggable `Publisher` with leases, retry with exponential
backoff, a dead-letter state, per-aggregate ordering and an idempotency key on every message. A sample consumer
shows how at-least-once delivery becomes effectively-once processing.

**What this is not:** there is **no Kafka, RabbitMQ or any real broker here**. The two `Publisher`
implementations are an in-memory test broker and a JSON-lines file. Everything runs on **H2 in memory in a single
JVM**; it has **not** been run against Postgres or a real broker (see [Known limits](#known-limits) and
[What H2 does and does not prove](#what-h2-does-and-does-not-prove)). All data is synthetic. It is a learning and
showcase project, not a production service.

## The pattern in one picture

```
POST /payments ──► [ one DB transaction ]                     relay (poll, claim, publish, mark)
                     INSERT payment            ┌──────────┐      claim ─► Publisher.publish ─► mark PUBLISHED
                     INSERT outbox  (NEW) ────►│  outbox  │◄──── retry: NEW + backoff    (crash here = duplicate,
                   [ commit or roll back both ]└──────────┘      dead:  DEAD (needs requeue)   consumer dedupes)
```

The point of the pattern: you never write to the database and to the broker as two independent steps (the dual
write problem). The broker write is retried from the database, so a crash or a broker outage loses nothing, at the
price of possible duplicates, which the consumer removes with the idempotency key.

## Endpoints

| Method and path | What it does | Success | Notable errors |
|---|---|---|---|
| `POST /payments` | Create `{accountId, amountMinor, currency}`; writes payment + `PaymentCreated` outbox row atomically | 201 + `Location` | 400 `validation` (field list) |
| `GET /payments/{id}` | One payment (`eventSeq` = events emitted so far) | 200 | 404 `not-found` |
| `POST /payments/{id}/capture` | CREATED to CAPTURED, emits `PaymentCaptured` | 200 | 404, 409 `conflict` |
| `POST /payments/{id}/refund` | CAPTURED to REFUNDED, emits `PaymentRefunded` | 200 | 404, 409 `conflict` |
| `GET /outbox/metrics` | `{pending, inFlight, published, dead, oldestUnpublishedAgeMillis}` | 200 | |
| `GET /admin/outbox?status=DEAD&limit=50` | Outbox rows by status (`NEW`, `CLAIMED`, `PUBLISHED`, `DEAD`), oldest id first | 200 | 400 bad status or limit (1..500) |
| `POST /admin/outbox/dead/requeue` | Move every DEAD row back to NEW with a fresh attempt budget | 200 `{requeued: n}` | |
| `POST /admin/outbox/{id}/requeue` | Same for one row | 200 | 404 unknown row, 409 row is not DEAD |
| `POST /admin/relay/poll` | Run one relay poll now, returns `{claimed, published, retried, dead, lostLease}` | 200 | |
| `GET /demo/broker/messages` | Everything the in-memory broker received (duplicates included) | 200 | 404 if `outbox.publisher=file` |
| `POST /demo/broker/fail?count=n` | Make the next n publish calls fail (to watch retry, dead letter, requeue) | 200 | 400 |
| `GET /demo/consumer` | State of the sample idempotent consumer | 200 | |

The admin and demo endpoints have **no authentication**. Errors are `application/problem+json` with a `code`.

State of an outbox row: `NEW` (due, or waiting out a backoff) to `CLAIMED` (leased to one relay) to `PUBLISHED`; a
failed attempt goes back to `NEW` with `next_attempt_at = now + backoff`, or to `DEAD` once `max-attempts` is reached.

## Delivery guarantees: what is and is not guaranteed

| Property | Guaranteed? | How, and where it is tested |
|---|---|---|
| Payment row and outbox row commit or roll back together | **Yes** (one DB transaction) | `PaymentServiceTest`: outbox insert failure rolls back the payment; failure in the caller's transaction after both writes leaves neither; a rolled-back capture leaves status and outbox untouched |
| A committed change is eventually published | **Yes, if** a relay keeps running and the publisher recovers within `max-attempts` | `OutboxRelayTest`, `RelayRaceTest` |
| Delivery semantics | **At-least-once**, not exactly-once | duplicates are demonstrated, not hidden (below) |
| Duplicate deliveries are possible | **Yes**: crash between publish and mark; a publish that outlives its lease | `OutboxRelayTest.aCrashBetweenPublishAndMark...`, `...OutlivesItsLease...`, `RelayRaceTest.aRelayThatDiesAfterPublishing...` |
| Every message has a stable idempotency key | **Yes**: `"<aggregateId>:<seq>"`, derived from the domain change, so it is identical on retry, crash replay and requeue | `PaymentServiceTest`, `OutboxRelayTest` |
| Consumer applies each key once | **Yes, for the sample consumer** (key inserted as a primary key in the same transaction as the effect) | `IdempotentConsumerTest`, `RelayRaceTest` (12 threads, same message: 1 applied) |
| Per-aggregate ordering of publication | **Yes**: a row is offered to a relay only when every earlier row of its aggregate is `PUBLISHED` | `OutboxRepositoryTest` (head-of-aggregate claim), `OutboxRelayTest`, `RelayRaceTest` (2 and 3 relays, flaky broker) |
| Ordering across different aggregates | **No** | by design |
| Two live relays do not both publish the same row | **Yes on H2 in this JVM** (observed over the test runs); the contract only promises at-least-once | `RelayRaceTest` asserts no duplicates when nothing crashes. Probabilistic evidence, see the H2 section |
| A crashed relay's rows are recovered | **Yes**, after the lease (`lease-millis`) expires | `OutboxRelayTest` with a fake clock, boundary included |
| A dead row blocks later events of its aggregate | **Yes (head-of-line blocking, on purpose)**: publishing seq 2 before a dead seq 1 would break ordering. Requeue releases it | `OutboxRelayTest.aDeadEventBlocksItsSuccessors...` |
| Ordering seen by a consumer | **Not guaranteed**: the relay orders publication; the broker and consumer may still reorder. Messages carry `aggregateSeq` so a consumer can check | not tested |
| Exactly-once | **No.** "Effectively-once" only where the consumer dedupes on the key | |
| Published rows are cleaned up | **No**: nothing ever deletes them (see Known limits) | |

### Retry, backoff, dead letter

`attempts` is incremented when a row is **claimed**, so a crashed attempt counts. On a failed publish the relay
computes `delay = min(max, base * 2^(attempts-1))` (overflow-safe) and sets `next_attempt_at = now + delay`. When
`attempts >= max-attempts` the row becomes `DEAD` and keeps its `last_error`. `POST /admin/outbox/.../requeue` resets
`attempts` to 0 and makes the row due immediately; the idempotency key does not change.

Defaults (`application.properties`, override on the command line): `outbox.batch-size=10`, `lease-millis=30000`,
`max-attempts=5`, `base-backoff-millis=1000`, `max-backoff-millis=60000`, `poll-interval-millis=500`,
`relay-id=relay-1` (must be unique per relay instance), `publisher=memory|file`, `file-path=outbox-broker.jsonl`.

### Claiming rows safely

```sql
UPDATE outbox SET status='CLAIMED', lease_owner=?, lease_until=?, attempts=attempts+1
WHERE id IN (SELECT o.id FROM outbox o
             WHERE ((o.status='NEW' AND o.next_attempt_at <= ?) OR (o.status='CLAIMED' AND o.lease_until <= ?))
               AND NOT EXISTS (SELECT 1 FROM outbox p WHERE p.aggregate_id=o.aggregate_id
                               AND p.aggregate_seq < o.aggregate_seq AND p.status <> 'PUBLISHED')
             ORDER BY o.id LIMIT ?)
  AND ((status='NEW' AND next_attempt_at <= ?) OR (status='CLAIMED' AND lease_until <= ?))   -- compare-and-set
```

The inner select picks candidates; the repeated condition on the outer `WHERE` is the compare-and-set: if another
relay claimed a row between the select and the update, the loser's update no longer matches it. Every relay uses a
fresh claim token (`relayId#uuid`) as `lease_owner`, and the final writes (`PUBLISHED`, retry, `DEAD`) are all
`... WHERE id=? AND status='CLAIMED' AND lease_owner=?`: a relay whose lease expired and was taken over cannot
overwrite the new owner's state (`lostLease` in the poll result). Only the **oldest unfinished row of each
aggregate** is offered, so a poll publishes at most one event per aggregate per round (a throughput trade-off for
ordering that does not need a per-aggregate lock).

### What H2 does and does not prove

- **Shown here:** the SQL is valid and the claim logic is correct on H2 2.x (MVStore, default `READ_COMMITTED`),
  where a concurrent `UPDATE` waits for the row lock and re-evaluates the `WHERE` afterwards. Real threads with
  separate pooled connections (`RelayRaceTest`) did not produce a double publish without a crash.
- **Not shown:** behaviour on Postgres/MySQL. Their locking, snapshot and `LIMIT`-in-subquery semantics differ; on
  Postgres I would use `SELECT ... FOR UPDATE SKIP LOCKED` and I have **not** tested that. Nothing here exercises
  network partitions, a database failover, clock skew between relay hosts (lease expiry uses each relay's own
  clock), or throughput.
- A race test that passes cannot prove a race is impossible; it only fails when the bad interleaving actually
  happens. See the mutation table for which kills are probabilistic.

## Try it (output below is from a real run of the jar)

```bash
mvn -q -B verify                       # builds target/spring-boot-outbox-relay-0.1.0.jar
java -jar target/spring-boot-outbox-relay-0.1.0.jar --outbox.base-backoff-millis=400 \
  --outbox.max-backoff-millis=400 --outbox.max-attempts=3 --outbox.poll-interval-millis=200    # port 8080
```

```bash
# 1. create a payment: the relay (every 200 ms) publishes it in the background
curl -s -i -X POST localhost:8080/payments -H 'Content-Type: application/json' \
  -d '{"accountId":"acct-42","amountMinor":2500,"currency":"USD"}'
# HTTP/1.1 201
# Location: /payments/pmt-3cbf57e5-80a9-49e8-b55e-feeccf445eff
# {"id":"pmt-3cbf57e5-...","accountId":"acct-42","amountMinor":2500,"currency":"USD","status":"CREATED","eventSeq":1,"createdAt":1792481520157}

# 2. capture it, then look at metrics, the broker and the consumer
curl -s -X POST localhost:8080/payments/pmt-3cbf57e5-80a9-49e8-b55e-feeccf445eff/capture
# {"id":"pmt-3cbf57e5-...","accountId":"acct-42","amountMinor":2500,"currency":"USD","status":"CAPTURED","eventSeq":2,...}
curl -s localhost:8080/outbox/metrics
# {"pending":0,"inFlight":0,"published":2,"dead":0,"oldestUnpublishedAgeMillis":null}
curl -s localhost:8080/demo/broker/messages        # two messages, keys pmt-3cbf57e5-...:1 and :2, "attempt":1
curl -s localhost:8080/demo/consumer
# {"processedMessages":2,"eventsApplied":1,"netCapturedMinorByAccount":{"acct-42":2500}}

# 3. errors
curl -s -X POST localhost:8080/payments/pmt-3cbf57e5-80a9-49e8-b55e-feeccf445eff/capture
# 409 {"type":"about:blank","title":"Conflict","status":409,"detail":"payment pmt-3cbf57e5-... is CAPTURED, expected CREATED",
#      "instance":"/payments/pmt-3cbf57e5-.../capture","code":"conflict"}
curl -s -X POST localhost:8080/payments -H 'Content-Type: application/json' -d '{"accountId":"","amountMinor":0,"currency":"usd"}'
# 400 {"type":"about:blank","title":"Bad Request","status":400,"detail":"Request validation failed","instance":"/payments",
#      "code":"validation","errors":[{"field":"accountId","message":"must not be blank"},
#      {"field":"amountMinor","message":"must be greater than 0"},{"field":"currency","message":"must match \"[A-Z]{3}\""}]}

# 4. broker outage: 3 injected failures with max-attempts=3 means the row goes DEAD
curl -s -X POST 'localhost:8080/demo/broker/fail?count=3'
# {"pendingFailures":3}
curl -s -X POST localhost:8080/payments -H 'Content-Type: application/json' -d '{"accountId":"acct-77","amountMinor":900,"currency":"EUR"}'
sleep 2; curl -s localhost:8080/outbox/metrics
# {"pending":0,"inFlight":0,"published":2,"dead":1,"oldestUnpublishedAgeMillis":null}
curl -s localhost:8080/admin/outbox
# [{"id":3,"aggregateId":"pmt-38e6e4cb-...","aggregateSeq":1,"eventType":"PaymentCreated","payload":"{...}",
#   "idempotencyKey":"pmt-38e6e4cb-...:1","status":"DEAD","attempts":3,"leaseOwner":null,"leaseUntil":null,
#   "lastError":"com.github.sup3rmar10.outbox.outbox.OutboxModel$PublishException: broker unavailable (injected)",...}]

# 5. a later event of the dead row's payment waits behind it (pending goes up, nothing is published)
curl -s -X POST localhost:8080/payments/pmt-38e6e4cb-7635-42f0-b289-d80b8fbe2432/capture >/dev/null; sleep 1
curl -s localhost:8080/outbox/metrics
# {"pending":1,"inFlight":0,"published":2,"dead":1,"oldestUnpublishedAgeMillis":2048}

# 6. requeue the dead row: it is delivered, then its successor follows
curl -s -X POST localhost:8080/admin/outbox/1/requeue          # a PUBLISHED row
# 409 {...,"detail":"outbox row 1 is PUBLISHED, only DEAD rows can be requeued","code":"conflict"}
curl -s -X POST localhost:8080/admin/outbox/3/requeue
# {"requeued":1}
sleep 1.5; curl -s localhost:8080/outbox/metrics
# {"pending":0,"inFlight":0,"published":4,"dead":0,"oldestUnpublishedAgeMillis":null}
curl -s localhost:8080/demo/consumer
# {"processedMessages":4,"eventsApplied":2,"netCapturedMinorByAccount":{"acct-42":2500,"acct-77":900}}
```

I also started the jar with `--outbox.publisher=file`: the relay appended one JSON line per message to
`outbox-broker.jsonl` (`{"outboxId":1,"aggregateId":"pmt-9a8b...","aggregateSeq":1,"eventType":"PaymentCreated",...,"attempt":1}`)
and `GET /demo/broker/messages` answered 404 as documented.

## Tests

`mvn -q -B verify` runs **99 tests in about 15 to 20 seconds**. Most tests are plain JUnit on a pooled in-memory H2
(no Spring context) so that the mutation check below stays affordable; two small `@SpringBootTest` classes cover the
HTTP layer and the background scheduler.

| Class | Tests | What it covers |
|---|---|---|
| `OutboxRepositoryTest` | 25 | claim protocol with explicit timestamps: due and lease boundaries, batch limit, head-of-aggregate rule, dead/backoff/claimed blocking, fenced writes, requeue, metrics |
| `OutboxRelayTest` | 22 | fake clock: publish, exact backoff times, max attempts, dead letter, ordering, crash and lease takeover, duplicate plus consumer dedupe, lost lease, restarted relay with the same id |
| `PaymentServiceTest` | 13 | atomic write, sequence numbers, state guards, validation, four rollback scenarios |
| `OutboxApiTest` | 13 | HTTP: lifecycle, validation, metrics, retry/dead/requeue flow, admin errors |
| `RelayRaceTest` | 5 | real threads: 2 and 3 relays racing, flaky broker, relay dying after publish, 12 threads delivering one message, 8 threads capturing one payment |
| `BackoffAndConfigTest` | 8 | backoff arithmetic and config validation |
| `IdempotentConsumerTest` | 6 | dedupe by key, refund sign, rollback of key when the effect fails |
| `BrokerTest` | 6 | in-memory and file brokers |
| `SchedulerTest` | 1 | the scheduled relay publishes without an explicit poll |

Time never comes from the wall clock in the deterministic tests: `Clock` is injected everywhere and tests use a
`MutableClock`. The race tests use the real clock and real threads and assert invariants, not interleavings.

## Mutation check

Passing tests prove little if they would also pass on broken code. I applied 31 behaviour-defining mutations one at
a time (textual replacements run by `scripts/mutation_check.py`; each match text must occur exactly once, the file is
restored in a `finally` block, and the whole suite runs for every mutation). It is a hand-rolled check, not PIT, and the
mutations are the ones I thought of; it says nothing about bugs I did not think of.

**Result: 30 of 31 killed, 1 survived (M08).** The full run took roughly 25 minutes of wall-clock time on a machine that
was also running other jobs.

| # | Mutation | Result | First failing tests |
|---|---|---|---|
| M01 | ordering gate removed: later events of an aggregate can be claimed before earlier ones | KILLED | OutboxRelayTest.aDeadEventBlocksItsSuccessorsUntilItIsRequeued, OutboxRelayTest.aLaterEventIsNotPublishedWhileAnEarlierOneOfTheSamePaymentIsFailing |
| M02 | a DEAD event no longer blocks its successors | KILLED | OutboxRepositoryTest.aDeadEventBlocksLaterEventsOfTheSameAggregateOnly |
| M03 | [race] claim loses its compare-and-set re-check (two relays can claim the same row) | KILLED | RelayRaceTest.aRelayThatDiesAfterPublishingLeavesADuplicateThatTheConsumerDedupes, RelayRaceTest.twoRelaysRacingPublishEveryRowAndTheConsumerAppliesEachOnce |
| M04 | expired leases are never taken over | KILLED | OutboxRelayTest.aCrashBetweenPublishAndMarkCausesADuplicateDeliveryThatTheConsumerDedupes, OutboxRelayTest.aRelayThatOutlivesItsLeaseLosesTheFinalWriteWhileTheNewOwnerPublishes |
| M05 | lease end is exclusive (a lease ending exactly now is still held) | KILLED | OutboxRelayTest.aCrashBetweenPublishAndMarkCausesADuplicateDeliveryThatTheConsumerDedupes, OutboxRelayTest.aRelayThatOutlivesItsLeaseLosesTheFinalWriteWhileTheNewOwnerPublishes |
| M06 | a row due exactly now is not claimed (<= becomes <) | KILLED | OutboxRelayTest.aCrashBetweenPublishAndMarkCausesADuplicateDeliveryThatTheConsumerDedupes, OutboxRelayTest.aDeadEventBlocksItsSuccessorsUntilItIsRequeued |
| M07 | claim does not count the attempt | KILLED | OutboxApiTest.aFailingBrokerLeadsToRetryThenDeadThenRequeueThenDelivery, OutboxApiTest.requeueAllMovesEveryDeadRow |
| M08 | claim has no ORDER BY (oldest-first is not enforced) | SURVIVED |  |
| M09 | markPublished no longer checks the lease owner | KILLED | OutboxRepositoryTest.anExpiredLeaseCanBeTakenOverAndTheLeaseEndIsInclusive, OutboxRepositoryTest.finalStateWritesAreFencedByTheClaimToken |
| M10 | markDead writes NEW instead of DEAD | KILLED | OutboxApiTest.aFailingBrokerLeadsToRetryThenDeadThenRequeueThenDelivery, OutboxApiTest.requeueAllMovesEveryDeadRow |
| M11 | requeue-all also touches rows that are not dead | KILLED | OutboxApiTest.requeueAllMovesEveryDeadRow, OutboxRelayTest.aDeadEventBlocksItsSuccessorsUntilItIsRequeued |
| M12 | requeue-one ignores the id and requeues every dead row | KILLED | OutboxRepositoryTest.requeueOneTouchesOnlyThatDeadRow |
| M13 | lag metric counts dead rows as unpublished | KILLED | OutboxRelayTest.metricsReflectPublishedPendingAndDeadRows, OutboxRepositoryTest.metricsCountEachStateAndReportTheOldestUnpublishedAge |
| M14 | batch size is ignored (x100) | KILLED | OutboxRelayTest.batchSizeLimitsTheRowsHandledPerPoll, OutboxRelayTest.twoRelaysPollingOneAfterTheOtherPublishEachRowOnce |
| M15 | no backoff: failed rows are retried at once | KILLED | OutboxRelayTest.aFailedPublishSchedulesARetryAfterTheBaseBackoff, OutboxRelayTest.aLaterEventIsNotPublishedWhileAnEarlierOneOfTheSamePaymentIsFailing |
| M16 | dead-letter one attempt late (>= becomes >) | KILLED | OutboxApiTest.aFailingBrokerLeadsToRetryThenDeadThenRequeueThenDelivery, OutboxApiTest.requeueAllMovesEveryDeadRow |
| M17 | claim token is just the relay id (a restarted relay re-reads its old leased rows) | KILLED | OutboxRelayTest.aRelayRestartedWithTheSameIdDoesNotReprocessRowsItsPreviousRunStillHasLeased, OutboxRelayTest.theLeaseOwnerNamesTheRelayAndTheLeaseEndsAfterTheConfiguredTime |
| M18 | lease length is zero (a lease expires the moment it is taken) | KILLED | OutboxRelayTest.aCrashBetweenPublishAndMarkCausesADuplicateDeliveryThatTheConsumerDedupes, OutboxRelayTest.aRelayRestartedWithTheSameIdDoesNotReprocessRowsItsPreviousRunStillHasLeased |
| M19 | payment service no longer joins/starts a transaction (SUPPORTS) | KILLED | PaymentServiceTest.ifTheOutboxInsertFailsThePaymentIsNotPersisted |
| M20 | payment service always opens its own transaction (REQUIRES_NEW) | KILLED | PaymentServiceTest.aRolledBackTransitionLeavesStatusAndOutboxAsTheyWere, PaymentServiceTest.ifTheCallersTransactionFailsAfterBothWritesNeitherPersists |
| M21 | status guard removed from the transition UPDATE | KILLED | OutboxApiTest.captureThenRefundWalkTheLifecycleAndRepeatsAre409, PaymentServiceTest.aRefusedTransitionChangesNeitherTheStatusNorTheOutbox |
| M22 | event sequence is not incremented on a transition | KILLED | OutboxApiTest.captureThenRefundWalkTheLifecycleAndRepeatsAre409, OutboxApiTest.pollDeliversToTheBrokerAndTheSampleConsumerAppliesTheCapture |
| M23 | amount of exactly zero accepted (<= becomes <) | KILLED | PaymentServiceTest.invalidInputIsRejectedBeforeAnythingIsWritten |
| M24 | event payload loses the amount | KILLED | OutboxApiTest.pollDeliversToTheBrokerAndTheSampleConsumerAppliesTheCapture, OutboxRelayTest.aCrashBetweenPublishAndMarkCausesADuplicateDeliveryThatTheConsumerDedupes |
| M25 | consumer applies the effect again when the key is a duplicate | KILLED | IdempotentConsumerTest.duplicatesAreRecognisedByKeyNotByContent, IdempotentConsumerTest.redeliveringTheSameKeyDoesNothing |
| M26 | a refund adds instead of subtracts | KILLED | IdempotentConsumerTest.aRefundSubtractsAndAccountsAreIndependent |
| M27 | POST /payments answers 200 instead of 201 | KILLED | OutboxApiTest.aFailingBrokerLeadsToRetryThenDeadThenRequeueThenDelivery, OutboxApiTest.adminListValidatesItsParameters |
| M28 | conflicts answer 400 instead of 409 | KILLED | OutboxApiTest.captureThenRefundWalkTheLifecycleAndRepeatsAre409, OutboxApiTest.requeueingARowThatIsNotDeadIs409AndAnUnknownRowIs404 |
| M29 | admin list defaults to NEW instead of DEAD | KILLED | OutboxApiTest.aFailingBrokerLeadsToRetryThenDeadThenRequeueThenDelivery |
| M30 | the background scheduler never calls the relay | KILLED | SchedulerTest.theScheduledRelayPublishesRowsWithoutAnyExplicitPoll |
| M31 | file broker truncates instead of appending | KILLED | BrokerTest.fileBrokerAppendsOneJsonLinePerMessageAndReadsThemBack |

**Survivor M08 (no `ORDER BY o.id` in the claim sub-select).** On H2 the sub-select scans the primary key, so rows come
back in id order with or without the clause; no H2-visible behaviour differs. After the first run I added
`claimTakesTheLowestIdFirstEvenWhenAHigherIdBecameDueEarlier`, hoping the `(status, next_attempt_at)` index would
expose the difference, and re-ran M08: it still survived (the new test passes with and without the clause, so it only
documents the contract). I am calling this an equivalent mutant on H2, not a test gap that I closed. On Postgres a
`LIMIT` without `ORDER BY` has no guaranteed order, so the clause matters there; I have not tested that.

**Probabilistic kill (M03).** Removing the compare-and-set re-check from the claim is killed only by the real-thread
tests (`RelayRaceTest`), never by a deterministic test, because a single thread cannot interleave two claims. It was
killed in 5 of 5 runs (the full run plus four repeats of M03 alone), but that is evidence about this machine and this
schedule, not a proof; a lucky interleaving could let the mutant pass. M19 and M20 (transaction propagation) and M17
(claim token) are killed by deterministic tests.

**Honest process notes.** (1) M17 is covered by a test I wrote while drafting the mutation list, before running it, so
it is a killed mutation by design, not a discovered gap. (2) My first mutation run was invalid: an aborted earlier
invocation had been killed mid-mutation and left one mutated line in `OutboxRepository.java`, so that run used a
tainted baseline (M01 and M02 reported BROKEN). I noticed from the BROKEN lines, restored the source by hand,
confirmed every match text occurs exactly once and `mvn verify` was green, and re-ran everything; the table above is
that clean second run.

## Known limits

- **H2 in memory, single JVM.** Not tested against Postgres, MySQL or a real broker; no failover, partition or
  clock-skew testing. See the H2 section above.
- **No Kafka.** The "file broker" is a JSON-lines appender (not fsynced); the in-memory broker is a test double.
- **At-least-once only.** Exactly-once is not claimed. A publish that outlives its lease, or a crash after publish,
  produces a duplicate; the consumer must dedupe on `idempotencyKey`.
- **One event per aggregate per poll round**, and a dead row blocks its aggregate until someone requeues it.
- **A poison message that crashes the JVM** counts its attempts at claim time, but a reclaimed row whose attempts are
  already at the limit still gets one more try before it can go `DEAD`.
- **Published rows are never deleted** (no retention job); the table grows without bound.
- **Admin and demo endpoints are unauthenticated.** Do not expose them.
- **Payload limit 4000 characters**, timestamps are epoch milliseconds from the relay's own clock.
- The sample consumer shares the producer's database only because this is a single-process demo; a real consumer
  has its own store, and its dedupe table needs retention too.
- Relay instances must use distinct `outbox.relay-id` values.

## Running and testing

Requirements: JDK 17+, Maven 3.9.

```bash
git clone https://github.com/Supp3r-Man/spring-boot-outbox-relay.git && cd spring-boot-outbox-relay
mvn -q -B verify                  # 99 tests, builds target/spring-boot-outbox-relay-0.1.0.jar
mvn spring-boot:run               # http://localhost:8080
```

CI: a GitHub Actions workflow is included (`.github/workflows/ci.yml`: `mvn -q -B verify` on Java 17 and 21);
CI ran green on the first run on GitHub Actions (Java 17 and 21, ubuntu-latest, 2026-10-20).

## Layout

```
src/main/java/com/github/sup3rmar10/outbox/
  payment/   PaymentService (the atomic write), Payment, PaymentStatus
  outbox/    OutboxRepository (all SQL), OutboxRelay, Backoff, RelayConfig, OutboxModel (Row, Message, Publisher, ...)
  publish/   InMemoryBroker, FileBroker
  consumer/  IdempotentConsumer
  config/    AppConfig, OutboxProperties, RelayScheduler
  web/       PaymentController, OutboxAdminController, DemoController, ApiExceptionHandler
src/main/resources/schema.sql    payment, outbox, processed_message, account_captured
scripts/mutation_check.py        the mutation check described above
```
