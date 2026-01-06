package io.velo;

/**
 * Thrown when attempting to compress or decompress data but dictionary is not available.
 * This exception indicates that a compression dictionary is required but is null or missing.
 */
public class DictMissingException extends RuntimeException {
    /**
     * Constructs a new DictMissingException with the specified detail message.
     *
     * @param message the detail message explaining the missing dictionary condition
     */
    public DictMissingException(String message) {
        super(message);
    }
}
