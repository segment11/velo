package io.velo.persist;

/**
 * Thrown when attempting to write to a slot that is in readonly mode.
 * This exception indicates that write operations are not permitted due to slot's readonly state.
 */
public class ReadonlyException extends RuntimeException {
}
