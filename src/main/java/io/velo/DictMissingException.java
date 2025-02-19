package io.velo;

/**
 * When do compress or decompress, throw this exception if dict is null.
 */
public class DictMissingException extends RuntimeException {
    public DictMissingException(String message) {
        super(message);
    }
}
