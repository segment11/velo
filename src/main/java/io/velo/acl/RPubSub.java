package io.velo.acl;

import io.velo.persist.KeyLoader;

public class RPubSub {
    public static final String ALL = "*";
    public static final String LITERAL_PREFIX = "&";

    String pattern;

    boolean check(String channel) {
        if (pattern.equals(ALL)) {
            return true;
        }
        return KeyLoader.isKeyMatch(channel, pattern);
    }

    String literal() {
        return LITERAL_PREFIX + pattern;
    }
}
