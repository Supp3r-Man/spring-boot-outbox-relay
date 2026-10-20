package com.github.sup3rmar10.outbox.outbox;

/** Exponential backoff without overflow: base, 2*base, 4*base ... capped at max. */
public final class Backoff {
    private Backoff() {
    }

    /** @param attempts number of delivery attempts made so far (1 after the first failure). */
    public static long delayMillis(int attempts, long baseMillis, long maxMillis) {
        if (baseMillis <= 0) {
            return 0;
        }
        int shift = Math.min(Math.max(attempts - 1, 0), 30);
        if (baseMillis > (maxMillis >> shift)) {
            return maxMillis;
        }
        return baseMillis << shift;
    }
}
