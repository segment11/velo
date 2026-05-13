package io.velo.acl;

import io.velo.persist.KeyLoader;

/**
 * Pattern for Redis Pub/Sub subscriptions.
 */
public class RPubSub {

    /** A special pattern that matches all channels. */
    public static final String ALL = "*";

    /** A prefix used to denote a literal pattern. */
    public static final String LITERAL_PREFIX = "&";

    /** The pattern used for matching channels. */
    String pattern;

    /**
     * @param channel the channel to check
     * @return true if the channel matches the pattern
     */
    boolean match(String channel) {
        if (pattern.equals(ALL)) {
            return true;
        }
        return KeyLoader.isKeyMatch(channel, pattern);
    }

    /** @return the literal representation of the pattern */
    String literal() {
        return LITERAL_PREFIX + pattern;
    }

    /**
     * @param str the string to check
     * @return true if the string is a valid RPubSub literal
     */
    public static boolean isRPubSubLiteral(String str) {
        return str.startsWith(LITERAL_PREFIX)
                || "allchannels".equals(str);
    }

    /**
     * @param str the literal string to convert
     * @return the RPubSub instance representing the literal pattern
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