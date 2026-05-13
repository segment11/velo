package io.velo.persist;

/**
 * Thrown when attempting to read from a slot that is a slave and not caught up.
 */
public class CannotReadException extends RuntimeException {
}
