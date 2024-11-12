package io.velo.acl;

// acl rule
public class RCmd {
    public static final String ALL = "*";
    public static final String ALLOW_LITERAL_PREFIX = "+";
    public static final String DISALLOW_LITERAL_PREFIX = "-";

    public static enum Type {
        cmd, cmd_with_first_arg, category, all
    }

    // false means deny, true means allow
    boolean allow;

    Type type;

    String cmd;

    // sub command
    String firstArg;

    Category category;

    boolean check(String cmd, String firstArg) {
        if (type == Type.all) {
            return allow;
        } else if (type == Type.cmd) {
            return this.cmd.equals(cmd) && allow;
        } else if (type == Type.cmd_with_first_arg) {
            return this.cmd.equals(cmd) && this.firstArg.equals(firstArg) && allow;
        } else {
            // type == Type.CATEGORY
            var categoryList = Category.getCategoryListByCmd(cmd);
            if (categoryList == null) {
                // velo extra commands
                return true;
            }

            for (var cmdCategory : categoryList) {
                if (this.category == cmdCategory) {
                    return allow;
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
}
