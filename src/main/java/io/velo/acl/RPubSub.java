package io.velo.acl;

import io.velo.persist.KeyLoader;

public class RPubSub {
    public static final String ALL = "*";
    public static final String LITERAL_PREFIX = "&";

    String pattern;

    boolean match(String channel) {
        if (pattern.equals(ALL)) {
            return true;
        }
        return KeyLoader.isKeyMatch(channel, pattern);
    }

    String literal() {
        return LITERAL_PREFIX + pattern;
    }

    public static boolean isRPubSubLiteral(String str) {
        return str.startsWith(LITERAL_PREFIX)
                || "allchannels".equals(str);
    }

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
