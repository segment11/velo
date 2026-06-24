package io.velo.persist;

import org.jetbrains.annotations.NotNull;

/**
 * Thrown when attempting to write to a chunk but all segments are full.
 */
public class SegmentOverflowException extends RuntimeException {
    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public SegmentOverflowException(@NotNull String message) {
        super(message);
    }
}
