package io.velo.acl;

import org.apache.commons.codec.digest.DigestUtils;

import java.util.ArrayList;
import java.util.List;

// acl user
public class U {
    public static final String DEFAULT_USER = "default";
    public static final String NO_PASS = "nopass";

    public static final String ADD_PASSWORD_PREFIX = ">";
    public static final String REMOVE_PASSWORD_PREFIX = "-";

    public static enum PasswordEncodedType {
        plain, sha256;
    }

    public static record Password(String passwordEncoded, PasswordEncodedType encodeType) {
        boolean isNoPass() {
            return NO_PASS.equals(this.passwordEncoded);
        }

        boolean check(String passwordRaw) {
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
    }

    final String user;

    public U(String user) {
        this.user = user;
    }

    boolean isOn = true;

    Password password;

    String literal() {
        var sb = new StringBuilder();
        sb.append("user ").append(user).append(" ");
        sb.append(isOn ? "on" : "off").append(" ");
        sb.append(password.isNoPass() ? NO_PASS : (ADD_PASSWORD_PREFIX + password.passwordEncoded)).append(" ");

        for (var rCmd : rCmdList) {
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

    final List<RCmd> rCmdList = new ArrayList<>();

    final List<RKey> rKeyList = new ArrayList<>();

    final List<RPubSub> rPubSubList = new ArrayList<>();
}
