package io.velo.acl;

import org.jetbrains.annotations.VisibleForTesting;

// acl rule
public class RCmd {
    public static final String ALL = "*";
    public static final String ALLOW_LITERAL_PREFIX = "+";
    public static final String DISALLOW_LITERAL_PREFIX = "-";

    public enum Type {
        cmd, cmd_with_first_arg, category, all
    }

    @VisibleForTesting
    boolean allow;

    @VisibleForTesting
    Type type;

    @VisibleForTesting
    // lower case
    String cmd;

    @VisibleForTesting
    // sub command, lower case
    String firstArg;

    @VisibleForTesting
    Category category;

    boolean match(String cmd, String firstArg) {
        if (type == Type.all) {
            return true;
        } else if (type == Type.cmd) {
            return this.cmd.equals(cmd);
        } else if (type == Type.cmd_with_first_arg) {
            if (firstArg == null) {
                return false;
            }
            return this.cmd.equals(cmd) && this.firstArg.equals(firstArg);
        } else {
            // type == Type.CATEGORY
            var categoryList = Category.getCategoryListByCmd(cmd);
            if (categoryList == null) {
                // velo extra commands
                return true;
            }

            for (var cmdCategory : categoryList) {
                if (this.category == cmdCategory) {
                    return true;
                }
            }
            return false;
        }
    }

    String literal() {
        if (type == Type.all) {
            return allow ? ALLOW_LITERAL_PREFIX + ALL : DISALLOW_LITERAL_PREFIX + ALL;
        } else if (type == Type.cmd) {
            return allow ? ALLOW_LITERAL_PREFIX + cmd : DISALLOW_LITERAL_PREFIX + cmd;
        } else if (type == Type.cmd_with_first_arg) {
            return allow ? ALLOW_LITERAL_PREFIX + cmd + "|" + firstArg : DISALLOW_LITERAL_PREFIX + cmd + "|" + firstArg;
        } else {
            return allow ? ALLOW_LITERAL_PREFIX + "@" + category.name() : DISALLOW_LITERAL_PREFIX + "@" + category.name();
        }
    }

    public static RCmd fromLiteral(String str) {
        var allow = str.startsWith(ALLOW_LITERAL_PREFIX);
        var disallow = str.startsWith(DISALLOW_LITERAL_PREFIX);
        if (!allow && !disallow) {
            throw new IllegalArgumentException("Invalid literal: " + str);
        }

        Type type;
        String cmd = ALL;
        String firstArg = null;
        Category category = null;
        if (str.contains("|")) {
            type = Type.cmd_with_first_arg;
            var parts = str.split("\\|");
            cmd = parts[0].substring(1);
            firstArg = parts[1];
        } else if (str.contains("@")) {
            type = Type.category;
            var parts = str.split("@");
            category = Category.valueOf(parts[1]);
        } else {
            type = str.contains("*") ? Type.all : Type.cmd;
            cmd = str.substring(1);
        }

        var rCmd = new RCmd();
        rCmd.allow = allow;
        rCmd.type = type;
        rCmd.cmd = cmd;
        rCmd.firstArg = firstArg;
        rCmd.category = category;
        return rCmd;
    }
}
