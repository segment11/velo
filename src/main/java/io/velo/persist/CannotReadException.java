package io.velo.persist;

/**
 * Thrown when attempting to read from a slot that can not be read.
 */
public class CannotReadException extends RuntimeException {
    /**
     * The message is not empty, the error reply of a command is built from the exception message.
     */
    public CannotReadException() {
        super("the slot can not read");
    }
}
