package io.velo.command;

/**
 * Exception thrown when a command returns an error reply.
 */
public class ErrorReplyException extends RuntimeException {
    public ErrorReplyException(String message) {
        super(message);
    }
}
