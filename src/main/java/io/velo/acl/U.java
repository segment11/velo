package io.velo.acl;

import io.velo.BaseCommand;
import org.apache.commons.codec.digest.DigestUtils;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

// acl user
public class U {
    public static final String DEFAULT_USER = "default";

    public static final String ADD_PASSWORD_PREFIX = ">";
    public static final String REMOVE_PASSWORD_PREFIX = "-";

    private enum PasswordEncodedType {
        plain, sha256;
    }

    public record Password(String passwordEncoded, PasswordEncodedType encodeType) {
        @VisibleForTesting
        boolean isNoPass() {
            return NO_PASS.equals(this.passwordEncoded);
        }

        public boolean check(String passwordRaw) {
            if (isNoPass()) {
                return true;
            }

            if (encodeType == PasswordEncodedType.plain) {
                return this.passwordEncoded.equals(passwordRaw);
            } else {
                var bytes = DigestUtils.sha256(passwordRaw);
                return this.passwordEncoded.equals(new String(bytes));
            }
        }

        public static Password plain(String password) {
            return new Password(password, PasswordEncodedType.plain);
        }

        public static Password sha256(String passwordRaw) {
            return new Password(new String(DigestUtils.sha256(passwordRaw)), PasswordEncodedType.sha256);
        }

        public static final String NO_PASS = "nopass";
        public static final Password NO_PASSWORD = new Password(NO_PASS, PasswordEncodedType.plain);
    }

    final String user;

    public U(String user) {
        this.user = user;
    }

    private boolean isOn = true;

    public boolean isOn() {
        return isOn;
    }

    public void setOn(boolean on) {
        isOn = on;
    }

    private Password password;

    public Password getPassword() {
        return password;
    }

    public void setPassword(Password password) {
        this.password = password;
    }

    public static final U INIT_DEFAULT_U = new U(DEFAULT_USER);

    static {
        INIT_DEFAULT_U.setPassword(Password.NO_PASSWORD);
    }

    public String literal() {
        var sb = new StringBuilder();
        sb.append("user ").append(user).append(" ");
        sb.append(isOn ? "on" : "off").append(" ");
        sb.append(password.isNoPass() ? Password.NO_PASS : (ADD_PASSWORD_PREFIX + password.passwordEncoded)).append(" ");

        for (var rCmd : rCmdList) {
            sb.append(rCmd.literal()).append(" ");
        }
        for (var rCmd : rCmdDisallowList) {
            sb.append(rCmd.literal()).append(" ");
        }
        for (var rKey : rKeyList) {
            sb.append(rKey.literal()).append(" ");
        }
        for (var rPubSub : rPubSubList) {
            sb.append(rPubSub.literal()).append(" ");
        }

        // remove last space
        sb.deleteCharAt(sb.length() - 1);
        return sb.toString();
    }

    @VisibleForTesting
    final List<RCmd> rCmdList = new ArrayList<>();
    @VisibleForTesting
    final List<RCmd> rCmdDisallowList = new ArrayList<>();

    @VisibleForTesting
    final List<RKey> rKeyList = new ArrayList<>();

    @VisibleForTesting
    final List<RPubSub> rPubSubList = new ArrayList<>();

    public void addRCmd(boolean clear, RCmd... rCmd) {
        if (clear) {
            rCmdList.clear();
        }
        rCmdList.addAll(Arrays.asList(rCmd));
    }

    public void addRCmdDisallow(boolean clear, RCmd... rCmd) {
        if (clear) {
            rCmdDisallowList.clear();
        }
        rCmdDisallowList.addAll(Arrays.asList(rCmd));
    }

    public void addRKey(boolean clear, RKey... rKey) {
        if (clear) {
            rKeyList.clear();
        }
        rKeyList.addAll(Arrays.asList(rKey));
    }

    public void addRPubSub(boolean clear, RPubSub... rPubSub) {
        if (clear) {
            rPubSubList.clear();
        }
        rPubSubList.addAll(Arrays.asList(rPubSub));
    }

    public boolean checkCmdAndKey(String cmd, byte[][] data, ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList) {
        if (rCmdList.isEmpty()) {
            return false;
        }

        var firstArg = data.length > 1 ? new String(data[1]).toLowerCase() : null;
        var isAllowCmdAnyOk = rCmdList.stream().anyMatch(rCmd -> rCmd.match(cmd, firstArg));
        if (!isAllowCmdAnyOk) {
            return false;
        }

        var isDisallowCmdEveryOk = rCmdDisallowList.stream().noneMatch(rCmd -> rCmd.match(cmd, firstArg));
        if (!isDisallowCmdEveryOk) {
            return false;
        }

        if (slotWithKeyHashList != null) {
            for (var slotWithKeyHash : slotWithKeyHashList) {
                for (var rKey : rKeyList) {
                    // todo, cmd read / write, need to check
                    if (!rKey.match(slotWithKeyHash.rawKey(), true, true)) {
                        return false;
                    }
                }
            }
        }

        return true;
    }

    public boolean checkChannels(String... channels) {
        if (rPubSubList.isEmpty()) {
            return false;
        }

        for (var rPubSub : rPubSubList) {
            for (var channel : channels) {
                if (!rPubSub.match(channel)) {
                    return false;
                }
            }
        }
        return true;
    }
}
