package io.velo.persist;

/**
 * Thrown when a big-string metadata entry references a file that does not exist on disk.
 * This indicates a data inconsistency where the key's metadata (WAL or persisted bucket)
 * claims a big-string value exists, but the underlying file has been lost or was never written.
 */
public class BigStringFileMissingException extends RuntimeException {
    /**
     * Constructs a new BigStringFileMissingException with the specified detail message.
     *
     * @param message the detail message explaining the missing file condition
     */
    public BigStringFileMissingException(String message) {
        super(message);
    }
}
