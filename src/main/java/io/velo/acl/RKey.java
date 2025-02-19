package io.velo.acl;

import io.velo.persist.KeyLoader;
import org.jetbrains.annotations.VisibleForTesting;

/**
 * Represents an access control key that defines permissions for reading and/or writing to specific patterns.
 */
public class RKey {
    /**
     * The literal prefix used to identify RKey literals.
     */
    public static final String LITERAL_PREFIX = "~";

    /**
     * Enumerates the possible types of permissions an RKey can have.
     */
    public enum Type {
        read, write, read_write, all
    }

    /**
     * The type of permission defined by this RKey.
     */
    @VisibleForTesting
    Type type;

    /**
     * The pattern that this RKey applies to.
     */
    @VisibleForTesting
    String pattern;

    /**
     * Checks if the given key matches the pattern of this RKey and if the command types (read/write) align with the RKey's permissions.
     *
     * @param key      The key to check against the pattern.
     * @param cmdRead  Whether the command is a read operation.
     * @param cmdWrite Whether the command is a write operation.
     * @return true if the key matches the pattern and the command types align with the RKey's permissions; false otherwise.
     */
    boolean match(String key, boolean cmdRead, boolean cmdWrite) {
        if (type == Type.all) {
            return true;
        } else if (type == Type.read) {
            return KeyLoader.isKeyMatch(key, pattern) && cmdRead;
        } else if (type == Type.write) {
            return KeyLoader.isKeyMatch(key, pattern) && cmdWrite;
        } else {
            // type == Type.read_write
            return KeyLoader.isKeyMatch(key, pattern);
        }
    }

    /**
     * Returns the literal string representation of this RKey, which can be used to recreate the RKey later.
     *
     * @return The literal string representation of this RKey.
     */
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
     * Determines whether the provided string is a valid RKey literal.
     *
     * @param str The string to check.
     * @return true if the string is a valid RKey literal; false otherwise.
     */
    public static boolean isRKeyLiteral(String str) {
        return str.startsWith(LITERAL_PREFIX)
                || str.startsWith("%R~")
                || str.startsWith("%W~")
                || str.startsWith("%RW~")
                || "allkeys".equals(str);
    }

    /**
     * Creates an RKey instance from its literal string representation.
     *
     * @param str The literal string representation of the RKey.
     * @return An RKey instance created from the literal.
     * @throws IllegalArgumentException if the provided string is not a valid RKey literal.
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