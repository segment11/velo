package io.velo.acl;

import io.velo.persist.KeyLoader;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Access control key defining permissions for reading and/or writing to patterns.
 */
public class RKey {
    /** The literal prefix used to identify RKey literals. */
    public static final String LITERAL_PREFIX = "~";

    /** Possible types of permissions. */
    public enum Type {
        read, write, read_write, all
    }

    /** The type of permission defined by this RKey. */
    @VisibleForTesting
    Type type;

    /** The pattern that this RKey applies to. */
    @VisibleForTesting
    String pattern;

    /**
     * @param key      the key to check against the pattern
     * @param cmdRead  whether the command is a read operation
     * @param cmdWrite whether the command is a write operation
     * @return true if the key matches and command types align with permissions
     */
    boolean match(String key, boolean cmdRead, boolean cmdWrite) {
        return match(key, cmdRead, cmdWrite, false);
    }

    /**
     * @param key       the key to check
     * @param cmdRead   whether the command is a read
     * @param cmdWrite  whether the command is a write
     * @param needsBoth whether both read and write are required
     * @return true if matched
     */
    boolean match(String key, boolean cmdRead, boolean cmdWrite, boolean needsBoth) {
        if (type == Type.all) {
            return true;
        } else if (type == Type.read) {
            return KeyLoader.isKeyMatch(key, pattern) && cmdRead && !needsBoth;
        } else if (type == Type.write) {
            return KeyLoader.isKeyMatch(key, pattern) && cmdWrite && !needsBoth;
        } else {
            return KeyLoader.isKeyMatch(key, pattern);
        }
    }

    /** @return the literal string representation */
    String literal() {
        if (type == Type.all) {
            return LITERAL_PREFIX + "*";
        } else if (type == Type.read) {
            return "%R" + LITERAL_PREFIX + pattern;
        } else if (type == Type.write) {
            return "%W" + LITERAL_PREFIX + pattern;
        } else {
            return "%RW" + LITERAL_PREFIX + pattern;
        }
    }

    /**
     * @param str the string to check
     * @return true if the string is a valid RKey literal
     */
    public static boolean isRKeyLiteral(String str) {
        return str.startsWith(LITERAL_PREFIX)
                || str.startsWith("%R~")
                || str.startsWith("%W~")
                || str.startsWith("%RW~")
                || "allkeys".equals(str);
    }

    /**
     * @param str the literal string representation of the RKey
     * @return the RKey instance created from the literal
     */
    public static RKey fromLiteral(String str) {
        if ("allkeys".equals(str)) {
            var rKey = new RKey();
            rKey.type = Type.all;
            rKey.pattern = "*";
            return rKey;
        }

        if (!str.contains(LITERAL_PREFIX)) {
            throw new IllegalArgumentException("Invalid literal: " + str);
        }

        Type type;
        String pattern;
        if (str.startsWith("%R~")) {
            type = Type.read;
            pattern = str.substring(3);
        } else if (str.startsWith("%W~")) {
            type = Type.write;
            pattern = str.substring(3);
        } else if (str.startsWith("%RW~")) {
            type = Type.read_write;
            pattern = str.substring(4);
        } else {
            if (!str.startsWith(LITERAL_PREFIX)) {
                throw new IllegalArgumentException("Invalid literal: " + str);
            }

            if (str.equals("~*")) {
                type = Type.all;
                pattern = "*";
            } else {
                type = Type.read_write;
                pattern = str.substring(1);
            }
        }

        var rKey = new RKey();
        rKey.type = type;
        rKey.pattern = pattern;
        return rKey;
    }
}