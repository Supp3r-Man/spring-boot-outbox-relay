package com.github.sup3rmar10.outbox.payment;

/** eventSeq is the number of events emitted for this payment so far (also the sequence of the latest one). */
public record Payment(String id, String accountId, long amountMinor, String currency, PaymentStatus status,
                      long eventSeq, long createdAt) {
}
