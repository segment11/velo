package io.velo.persist;

/**
 * Thrown when key bucket cells are all full during write operations.
 */
public class BucketFullException extends RuntimeException {
    public BucketFullException(String message) {
        super(message);
    }
}
