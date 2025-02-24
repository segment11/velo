package io.velo.persist;

/**
 * Thrown when do keys write, if key buckets cells are all full.
 */
public class BucketFullException extends RuntimeException {
    public BucketFullException(String message) {
        super(message);
    }
}
