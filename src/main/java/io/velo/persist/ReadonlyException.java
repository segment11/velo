package io.velo.persist;

/**
 * Thrown when attempting to write to a slot that is in readonly mode.
 */
public class ReadonlyException extends RuntimeException {
    /**
     * The message is not empty, the error reply of a command is built from the exception message.
     */
    public ReadonlyException() {
        super("the slot is readonly, the write is rejected");
    }
}
