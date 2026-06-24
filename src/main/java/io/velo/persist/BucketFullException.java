package io.velo.persist;

/**
 * Thrown when key bucket cells are all full during write operations.
 */
public class BucketFullException extends RuntimeException {
    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public BucketFullException(String message) {
        super(message);
    }
}
