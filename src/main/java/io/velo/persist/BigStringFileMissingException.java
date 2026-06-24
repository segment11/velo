package io.velo.persist;

/**
 * Thrown when a big-string metadata entry references a file that does not exist on disk.
 */
public class BigStringFileMissingException extends RuntimeException {
    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public BigStringFileMissingException(String message) {
        super(message);
    }
}
