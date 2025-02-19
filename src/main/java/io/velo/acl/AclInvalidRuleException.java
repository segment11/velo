package io.velo.acl;

/**
 * Exception thrown when an ACL rule is invalid.
 */
public class AclInvalidRuleException extends RuntimeException {
    public AclInvalidRuleException(String message) {
        super(message);
    }
}
