package io.velo.persist;

import org.jetbrains.annotations.NotNull;

/**
 * Thrown when attempting to write to a chunk but all segments are full.
 * This exception indicates that storage capacity has been exhausted and no additional segments are available.
 */
public class SegmentOverflowException extends RuntimeException {
    /**
     * Constructs a new SegmentOverflowException with the specified detail message.
     *
     * @param message the detail message explaining the overflow condition
     */
    public SegmentOverflowException(@NotNull String message) {
        super(message);
    }
}
