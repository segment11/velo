package io.velo.persist;

/**
 * Thrown when attempting to read from a slot that is slave and not caught up.
 * This exception indicates that read operations are not permitted due to slot's can not read state.
 */
public class CannotReadException extends RuntimeException {
}
