package io.velo.persist;

import org.jetbrains.annotations.NotNull;

public class SegmentOverflowException extends RuntimeException {
    public SegmentOverflowException(@NotNull String message) {
        super(message);
    }
}
