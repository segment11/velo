package io.velo.persist;

import org.jetbrains.annotations.NotNull;

/**
 * Estimates in-memory usage.
 */
public interface InMemoryEstimate {
    long estimate(@NotNull StringBuilder sb);
}
