package io.velo.acl;

/**
 * Exception thrown when an ACL rule is invalid.
 */
public class AclInvalidRuleException extends RuntimeException {
    /**
     * @param message the detail message describing the invalid rule
     */
    public AclInvalidRuleException(String message) {
        super(message);
    }
}
