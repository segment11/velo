package io.velo.persist;

public class BucketFullException extends RuntimeException {
    public BucketFullException(String message) {
        super(message);
    }
}
