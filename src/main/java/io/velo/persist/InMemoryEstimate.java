package io.velo.persist;

import org.jetbrains.annotations.NotNull;

/**
 * Interface for estimate memory usage.
 */
public interface InMemoryEstimate {
    long estimate(@NotNull StringBuilder sb);
}
