package io.velo.persist;

import org.jetbrains.annotations.NotNull;

public interface InMemoryEstimate {
    long estimate(@NotNull StringBuilder sb);
}
