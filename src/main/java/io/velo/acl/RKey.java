package io.velo.acl;

import io.velo.persist.KeyLoader;

public class RKey {
    public static final String LITERAL_PREFIX = "~";

    public static enum Type {
        read, write, read_write, all
    }

    Type type;

    String pattern;

    boolean check(String key, boolean cmdRead, boolean cmdWrite) {
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
}
