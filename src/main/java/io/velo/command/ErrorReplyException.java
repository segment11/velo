package io.velo.command;

/**
 * Exception thrown when a command returns an error reply.
 */
public class ErrorReplyException extends RuntimeException {
    /**
     * @param message the error message
     */
    public ErrorReplyException(String message) {
        super(message);
    }
}
