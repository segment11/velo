package io.velo.persist;

/**
 * Thrown when a PersistValueMeta byte payload contains out-of-range field values,
 * indicating corruption in the stored PVM cell.
 */
public class PersistValueMetaCorruptedException extends RuntimeException {
    public PersistValueMetaCorruptedException(String message) {
        super(message);
    }
}
