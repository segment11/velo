package io.velo.acl;

import io.velo.persist.KeyLoader;
import org.jetbrains.annotations.VisibleForTesting;

public class RKey {
    public static final String LITERAL_PREFIX = "~";

    public enum Type {
        read, write, read_write, all
    }

    @VisibleForTesting
    Type type;

    @VisibleForTesting
    String pattern;

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

    public static boolean isRKeyLiteral(String str) {
        return str.startsWith(LITERAL_PREFIX)
                || str.startsWith("%R~")
                || str.startsWith("%W~")
                || str.startsWith("%RW~")
                || "allkeys".equals(str);
    }

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
