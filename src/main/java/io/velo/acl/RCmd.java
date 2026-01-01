package io.velo.acl;

import org.jetbrains.annotations.VisibleForTesting;

/**
 * Represents an access control rule for commands.
 * Each rule can be of different types (cmd, cmd_with_first_arg, category, all)
 * and can either allow or disallow the execution of commands.
 */
public class RCmd {
    public static final String ALL = "*";
    public static final String ALLOW_LITERAL_PREFIX = "+";
    public static final String DISALLOW_LITERAL_PREFIX = "-";

    /**
     * Types of rules that RCmd can represent.
     */
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

    /**
     * Checks if the given command and first argument match this rule.
     *
     * @param cmd      the command to check
     * @param firstArg the first argument of the command to check
     * @return true if the command and argument match the rule, false otherwise
     */
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
            if (category == Category.all) {
                return true;
            }

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

    /**
     * Converts the rule to a literal string representation.
     *
     * @return the string representing the literal form of the rule
     */
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

    /**
     * Checks if a given literal string represents an allow rule.
     *
     * @param str the literal string to check
     * @return true if the string represents an allow rule, false otherwise
     */
    public static boolean isAllowLiteral(String str) {
        return str.startsWith(ALLOW_LITERAL_PREFIX) || "allcommands".equals(str);
    }

    /**
     * Checks if a given literal string is a valid RCmd literal.
     *
     * @param str the literal string to check
     * @return true if the string is a valid RCmd literal, false otherwise
     */
    public static boolean isRCmdLiteral(String str) {
        return str.startsWith(ALLOW_LITERAL_PREFIX)
                || str.startsWith(DISALLOW_LITERAL_PREFIX)
                || "allcommands".equals(str)
                || "nocommands".equals(str);
    }

    /**
     * Creates an RCmd object from a literal string.
     *
     * @param str the literal string to convert
     * @return the RCmd object representing the literal string
     * @throws IllegalArgumentException if the literal string is invalid
     */
    public static RCmd fromLiteral(String str) {
        if ("allcommands".equals(str)) {
            var rCmd = new RCmd();
            rCmd.allow = true;
            rCmd.type = Type.category;
            rCmd.category = Category.all;
            return rCmd;
        }

        if ("nocommands".equals(str)) {
            var rCmd = new RCmd();
            rCmd.allow = false;
            rCmd.type = Type.category;
            rCmd.category = Category.all;
            return rCmd;
        }

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
            category = parts[1].equals("*") ? Category.all : Category.valueOf(parts[1]);
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