package io.velo.acl;

import io.velo.persist.KeyLoader;

/**
 * Represents a pattern for Redis Pub/Sub subscriptions.
 * It can match specific channels or use a wildcard to match all channels.
 */
public class RPubSub {

    /**
     * A special pattern that matches all channels.
     */
    public static final String ALL = "*";

    /**
     * A prefix used to denote a literal pattern.
     */
    public static final String LITERAL_PREFIX = "&";

    /**
     * The pattern used for matching channels.
     */
    String pattern;

    /**
     * Checks if the given channel matches the pattern.
     *
     * @param channel the channel to check
     * @return true if the channel matches the pattern, false otherwise
     */
    boolean match(String channel) {
        if (pattern.equals(ALL)) {
            return true;
        }
        return KeyLoader.isKeyMatch(channel, pattern);
    }

    /**
     * Returns the literal representation of the pattern.
     * The literal is prefixed with {@link #LITERAL_PREFIX}.
     *
     * @return the literal representation of the pattern
     */
    String literal() {
        return LITERAL_PREFIX + pattern;
    }

    /**
     * Checks if the given string is a valid literal pattern for RPubSub.
     * A valid literal starts with {@link #LITERAL_PREFIX} or is equal to "allchannels".
     *
     * @param str the string to check
     * @return true if the string is a valid RPubSub literal, false otherwise
     */
    public static boolean isRPubSubLiteral(String str) {
        return str.startsWith(LITERAL_PREFIX)
                || "allchannels".equals(str);
    }

    /**
     * Creates an RPubSub instance from a literal string.
     * The literal string should be in the format defined by {@link #LITERAL_PREFIX}
     * or should be equal to "allchannels".
     *
     * @param str the literal string to convert
     * @return the RPubSub instance representing the literal pattern
     * @throws IllegalArgumentException if the given string is not a valid RPubSub literal
     */
    public static RPubSub fromLiteral(String str) {
        if ("allchannels".equals(str)) {
            var rPubSub = new RPubSub();
            rPubSub.pattern = ALL;
            return rPubSub;
        }

        if (!str.contains(LITERAL_PREFIX)) {
            throw new IllegalArgumentException("Invalid literal: " + str);
        }

        var pattern = str.substring(1);
        var rPubSub = new RPubSub();
        rPubSub.pattern = pattern;
        return rPubSub;
    }
}