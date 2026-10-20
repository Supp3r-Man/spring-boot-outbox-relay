#!/usr/bin/env python
"""Hand-rolled mutation check: apply one textual mutation at a time, run the test suite,
record whether at least one test failed, then restore the original file.

Usage:  python scripts/mutation_check.py             (all mutations)
        python scripts/mutation_check.py M05 M10     (selected ids)
        python scripts/mutation_check.py --list      (print the mutations, run nothing)
Set MVN to the Maven executable if `mvn` is not on PATH.

Each mutation replaces the ONLY occurrence of `old` in the named file with `new`. A mutation whose
`old` text is missing or appears more than once is reported as BROKEN (the script is out of date),
never as a survivor. Files are always restored from the original bytes, even on Ctrl-C.
The Spring tests run once per mutation; expect about 20 seconds each (roughly 10 minutes in total).

Mutations marked [race] can only be killed by the real-thread tests, which assert invariants rather than a
fixed interleaving: a kill there is probabilistic (a lucky schedule could let the mutant pass).
"""
import os
import re
import subprocess
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src", "main", "java", "com", "github", "sup3rmar10", "outbox")
MVN = os.environ.get("MVN", "mvn")

REPO = "outbox/OutboxRepository.java"
RELAY = "outbox/OutboxRelay.java"
PAY = "payment/PaymentService.java"

# (id, description, relative path under SRC, old text, new text)
MUTATIONS = [
    # --- claim protocol and per-aggregate ordering (SQL)
    ("M01", "ordering gate removed: later events of an aggregate can be claimed before earlier ones", REPO,
     "p.status <> 'PUBLISHED'", "p.status = 'NEVER'"),
    ("M02", "a DEAD event no longer blocks its successors", REPO,
     "p.status <> 'PUBLISHED'", "p.status IN ('NEW', 'CLAIMED')"),
    ("M03", "[race] claim loses its compare-and-set re-check (two relays can claim the same row)", REPO,
     "AND ((status = 'NEW' AND next_attempt_at <= ?) OR (status = 'CLAIMED' AND lease_until <= ?))",
     "AND (? >= 0 AND ? >= 0)"),
    ("M04", "expired leases are never taken over", REPO,
     "OR (o.status = 'CLAIMED' AND o.lease_until <= ?))", "OR (o.status = 'CLAIMED' AND o.lease_until <= ? AND 1 = 0))"),
    ("M05", "lease end is exclusive (a lease ending exactly now is still held)", REPO,
     "o.lease_until <= ?", "o.lease_until < ?"),
    ("M06", "a row due exactly now is not claimed (<= becomes <)", REPO,
     "(o.status = 'NEW' AND o.next_attempt_at <= ?)", "(o.status = 'NEW' AND o.next_attempt_at < ?)"),
    ("M07", "claim does not count the attempt", REPO,
     "attempts = attempts + 1", "attempts = attempts + 0"),
    ("M08", "claim has no ORDER BY (oldest-first is not enforced)", REPO,
     "ORDER BY o.id\n              LIMIT ?)", "LIMIT ?)"),
    # --- fenced state writes, requeue, metrics
    ("M09", "markPublished no longer checks the lease owner", REPO,
     "AND status = 'CLAIMED' AND lease_owner = ?\", now, id, token) == 1;",
     "AND status = 'CLAIMED' AND ? IS NOT NULL\", now, id, token) == 1;"),
    ("M10", "markDead writes NEW instead of DEAD", REPO,
     "UPDATE outbox SET status = 'DEAD', last_error = ?", "UPDATE outbox SET status = 'NEW', last_error = ?"),
    ("M11", "requeue-all also touches rows that are not dead", REPO,
     "+ \" WHERE status = 'DEAD'\", now);", "+ \" WHERE status IN ('DEAD', 'NEW')\", now);"),
    ("M12", "requeue-one ignores the id and requeues every dead row", REPO,
     "WHERE status = 'DEAD' AND id = ?\", now, id);", "WHERE status = 'DEAD' AND ? > 0\", now, id);"),
    ("M13", "lag metric counts dead rows as unpublished", REPO,
     "WHERE status IN ('NEW', 'CLAIMED')\", Long.class);", "WHERE status IN ('NEW', 'CLAIMED', 'DEAD')\", Long.class);"),
    # --- relay
    ("M14", "batch size is ignored (x100)", RELAY,
     "now + config.leaseMillis(), config.batchSize());", "now + config.leaseMillis(), config.batchSize() * 100);"),
    ("M15", "no backoff: failed rows are retried at once", RELAY,
     "long next = clock.millis()\n                            + Backoff.delayMillis(row.attempts(), config.baseBackoffMillis(), config.maxBackoffMillis());",
     "long next = clock.millis();"),
    ("M16", "dead-letter one attempt late (>= becomes >)", RELAY,
     "row.attempts() >= config.maxAttempts()", "row.attempts() > config.maxAttempts()"),
    ("M17", "claim token is just the relay id (a restarted relay re-reads its old leased rows)", RELAY,
     "String token = config.relayId() + \"#\" + UUID.randomUUID();", "String token = config.relayId();"),
    ("M18", "lease length is zero (a lease expires the moment it is taken)", RELAY,
     "now + config.leaseMillis(), config.batchSize());", "now, config.batchSize());"),
    # --- producer: the atomic write
    ("M19", "payment service no longer joins/starts a transaction (SUPPORTS)", PAY,
     "TransactionDefinition.PROPAGATION_REQUIRED", "TransactionDefinition.PROPAGATION_SUPPORTS"),
    ("M20", "payment service always opens its own transaction (REQUIRES_NEW)", PAY,
     "TransactionDefinition.PROPAGATION_REQUIRED", "TransactionDefinition.PROPAGATION_REQUIRES_NEW"),
    ("M21", "status guard removed from the transition UPDATE", PAY,
     "WHERE id = ? AND status = ?\",", "WHERE id = ? AND ? IS NOT NULL\","),
    ("M22", "event sequence is not incremented on a transition", PAY,
     "event_seq = event_seq + 1", "event_seq = event_seq + 0"),
    ("M23", "amount of exactly zero accepted (<= becomes <)", PAY,
     "if (amountMinor <= 0) {", "if (amountMinor < 0) {"),
    ("M24", "event payload loses the amount", PAY,
     "        body.put(\"amountMinor\", p.amountMinor());\n", ""),
    # --- consumer
    ("M25", "consumer applies the effect again when the key is a duplicate", "consumer/IdempotentConsumer.java",
     "} catch (DuplicateKeyException duplicate) {", "} catch (DuplicateKeyException duplicate) {\n                apply(message);"),
    ("M26", "a refund adds instead of subtracts", "consumer/IdempotentConsumer.java",
     "case \"PaymentRefunded\" -> -1;", "case \"PaymentRefunded\" -> 1;"),
    # --- web, scheduler, file broker
    ("M27", "POST /payments answers 200 instead of 201", "web/PaymentController.java",
     "ResponseEntity.created(URI.create(\"/payments/\" + p.id())).body(p)", "ResponseEntity.ok().body(p)"),
    ("M28", "conflicts answer 400 instead of 409", "web/ApiExceptionHandler.java",
     "return problem(HttpStatus.CONFLICT, \"conflict\", ex.getMessage());",
     "return problem(HttpStatus.BAD_REQUEST, \"conflict\", ex.getMessage());"),
    ("M29", "admin list defaults to NEW instead of DEAD", "web/OutboxAdminController.java",
     "@RequestParam(defaultValue = \"DEAD\")", "@RequestParam(defaultValue = \"NEW\")"),
    ("M30", "the background scheduler never calls the relay", "config/RelayScheduler.java",
     "relay.pollOnce();", ""),
    ("M31", "file broker truncates instead of appending", "publish/FileBroker.java",
     "StandardOpenOption.CREATE, StandardOpenOption.APPEND", "StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING"),
]


def run_tests():
    try:
        p = subprocess.run([MVN, "-B", "-q", "test"], cwd=ROOT, capture_output=True, text=True, timeout=900)
    except subprocess.TimeoutExpired:
        return "timeout", []
    out = p.stdout + p.stderr
    if "COMPILATION ERROR" in out:
        return "compile-error", []
    failing = sorted(set(re.findall(r"\[ERROR\]\s{2,}(\w+Test\.\w+)", out)))
    return ("killed" if p.returncode != 0 else "survived"), failing


def main():
    args = sys.argv[1:]
    if "--list" in args:
        for mid, desc, fname, _old, _new in MUTATIONS:
            print(f"{mid} | {desc} | {fname}")
        return
    wanted = set(args)
    originals = {}
    results = []
    try:
        for mid, desc, fname, old, new in MUTATIONS:
            if wanted and mid not in wanted:
                continue
            path = os.path.join(SRC, *fname.split("/"))
            if fname not in originals:
                with open(path, "rb") as f:
                    originals[fname] = f.read()
            text = originals[fname].decode("utf-8").replace("\r\n", "\n")
            if text.count(old) != 1:
                label = "BROKEN (pattern not found)" if old not in text else "BROKEN (pattern not unique)"
                results.append((mid, desc, label, []))
                print(f"{mid} | {desc} | {label}", flush=True)
                continue
            with open(path, "w", encoding="utf-8", newline="\n") as f:
                f.write(text.replace(old, new, 1))
            try:
                status, failing = run_tests()
            finally:
                with open(path, "wb") as f:
                    f.write(originals[fname])
            label = {"killed": "KILLED", "survived": "SURVIVED", "compile-error": "COMPILE ERROR (invalid)",
                     "timeout": "KILLED (timeout)"}[status]
            results.append((mid, desc, label, failing))
            print(f"{mid} | {desc} | {label} | {len(failing)} failing: {', '.join(failing[:3])}", flush=True)
    finally:
        for fname, data in originals.items():
            with open(os.path.join(SRC, *fname.split("/")), "wb") as f:
                f.write(data)
    killed = sum(1 for r in results if r[2].startswith("KILLED"))
    print("\n| id | mutation | result | first failing tests |")
    print("|---|---|---|---|")
    for mid, desc, label, failing in results:
        print(f"| {mid} | {desc} | {label} | {', '.join(failing[:2])} |")
    print(f"\n{killed}/{len(results)} mutations killed")


if __name__ == "__main__":
    main()
