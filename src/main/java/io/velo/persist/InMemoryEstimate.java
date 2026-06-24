package io.velo.persist;

import org.jetbrains.annotations.NotNull;

/**
 * Estimates in-memory usage.
 */
public interface InMemoryEstimate {
    /**
     * Appends a human-readable estimate description to the given builder and returns the
     * estimated in-memory capacity in bytes.
     *
     * @param sb the builder to append details to
     * @return the estimated in-memory capacity in bytes
     */
    long estimate(@NotNull StringBuilder sb);
}
