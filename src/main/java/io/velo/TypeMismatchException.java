package io.velo;

/**
 * Exception thrown when attempting to perform an operation on a value that does not match the expected type.
 * This exception is used to indicate type violations in operations that require specific data types.
 */
public class TypeMismatchException extends RuntimeException {
    /**
     * Constructs a new TypeMismatchException with the specified detail message.
     *
     * @param message the detail message explaining the type mismatch
     */
    public TypeMismatchException(String message) {
        super(message);
    }
}
