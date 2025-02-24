package io.velo.persist;

import org.jetbrains.annotations.NotNull;

/**
 * Thrown when do chunk write, if segments are all full.
 */
public class SegmentOverflowException extends RuntimeException {
    public SegmentOverflowException(@NotNull String message) {
        super(message);
    }
}
