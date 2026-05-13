package io.velo.persist;

/**
 * Thrown when a big-string metadata entry references a file that does not exist on disk.
 */
public class BigStringFileMissingException extends RuntimeException {
    public BigStringFileMissingException(String message) {
        super(message);
    }
}
