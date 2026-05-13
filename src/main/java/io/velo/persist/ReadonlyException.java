package io.velo.persist;

/**
 * Thrown when attempting to write to a slot that is in readonly mode.
 */
public class ReadonlyException extends RuntimeException {
}
