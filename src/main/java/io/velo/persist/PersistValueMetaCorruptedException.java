package io.velo.persist;

/**
 * Thrown when a PersistValueMeta byte payload contains out-of-range field values,
 * indicating corruption in the stored PVM cell.
 */
public class PersistValueMetaCorruptedException extends RuntimeException {
    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message the detail message
     */
    public PersistValueMetaCorruptedException(String message) {
        super(message);
    }
}
