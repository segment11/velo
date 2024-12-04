package io.velo.acl;

import io.velo.BaseCommand;
import io.velo.reply.BulkReply;
import io.velo.reply.MultiBulkReply;
import io.velo.reply.Reply;
import org.apache.commons.codec.digest.DigestUtils;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.velo.acl.U.CheckCmdAndKeyResult.FALSE_WHEN_CHECK_CMD;
import static io.velo.acl.U.CheckCmdAndKeyResult.FALSE_WHEN_CHECK_KEY;

// acl user
public class U {
    public static final String DEFAULT_USER = "default";

    public static final String ADD_PASSWORD_PREFIX = ">";
    public static final String REMOVE_PASSWORD_PREFIX = "<";
    public static final String ADD_HASH_PASSWORD_PREFIX = "#";
    public static final String REMOVE_HASH_PASSWORD_PREFIX = "!";

    private enum PasswordEncodedType {
        plain, sha256Hex;
    }

    public record Password(String passwordEncoded, PasswordEncodedType encodeType) {
        @VisibleForTesting
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
                return this.passwordEncoded.equals(DigestUtils.sha256Hex(passwordRaw));
            }
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            var password = (Password) obj;
            return passwordEncoded.equals(password.passwordEncoded) && encodeType == password.encodeType;
        }

        public static Password plain(String passwordRaw) {
            return new Password(passwordRaw, PasswordEncodedType.plain);
        }

        public static Password sha256Hex(String passwordRaw) {
            return new Password(DigestUtils.sha256Hex(passwordRaw), PasswordEncodedType.sha256Hex);
        }

        public static Password sha256HexEncoded(String passwordSha256Hex) {
            return new Password(passwordSha256Hex, PasswordEncodedType.sha256Hex);
        }

        public static final String NO_PASS = "nopass";
        public static final Password NO_PASSWORD = new Password(NO_PASS, PasswordEncodedType.plain);
    }

    final String user;

    public String getUser() {
        return user;
    }

    public U(String user) {
        this.user = user;
    }

    // new user default off
    private boolean isOn = false;

    public boolean isOn() {
        return isOn;
    }

    public void setOn(boolean on) {
        isOn = on;
    }

    private final ArrayList<Password> passwords = new ArrayList<>();

    public void addPassword(Password password) {
        // check duplicate
        if (passwords.stream().anyMatch(p -> p.equals(password))) {
            return;
        }
        passwords.add(password);
    }

    public void removePassword(Password password) {
        passwords.stream().filter(p -> p.equals(password)).findFirst().ifPresent(passwords::remove);
    }

    public void resetPassword() {
        passwords.clear();
    }

    public boolean checkPassword(String passwordRaw) {
        // reset password
        if (passwords.isEmpty()) {
            return false;
        }
        return passwords.stream().anyMatch(password -> password.check(passwordRaw));
    }

    @TestOnly
    public void setPassword(Password password) {
        passwords.clear();
        passwords.add(password);
    }

    @TestOnly
    public Password getFirstPassword() {
        return passwords.isEmpty() ? null : passwords.getFirst();
    }

    public static final U INIT_DEFAULT_U = new U(DEFAULT_USER);

    static {
        INIT_DEFAULT_U.setOn(true);
        INIT_DEFAULT_U.addPassword(Password.NO_PASSWORD);
        INIT_DEFAULT_U.addRCmd(true, RCmd.fromLiteral("+*"), RCmd.fromLiteral("+@all"));
        INIT_DEFAULT_U.addRKey(true, RKey.fromLiteral("~*"));
        INIT_DEFAULT_U.addRPubSub(true, RPubSub.fromLiteral("&*"));
    }

    public String literal() {
        var sb = new StringBuilder();
        sb.append("user ").append(user).append(" ");
        sb.append(isOn ? "on" : "off").append(" ");
        var firstPassword = getFirstPassword();
        // need # before password ? todo
        sb.append(firstPassword.passwordEncoded).append(" ");

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

    // for acl getuser
    public Reply[] toReplies() {
        var replies = new Reply[10];
        replies[0] = new BulkReply("flags".getBytes());

        List<String> flags = new ArrayList<>();
        flags.add(isOn ? "on" : "off");
        if (rCmdList.stream().anyMatch(rCmd -> rCmd.type == RCmd.Type.all ||
                (rCmd.type == RCmd.Type.category && rCmd.category == Category.all)) && rCmdDisallowList.isEmpty()) {
            flags.add("allcommands");
        }
        if (rKeyList.stream().anyMatch(rKey -> rKey.type == RKey.Type.all)) {
            flags.add("allkeys");
        }
        if (rPubSubList.stream().anyMatch(rPubSub -> rPubSub.pattern.equals(RPubSub.ALL))) {
            flags.add("allchannels");
        }
        if (passwords.stream().anyMatch(Password::isNoPass)) {
            flags.add("nopass");
        }
        var flagsReplies = new Reply[flags.size()];
        for (int i = 0; i < flags.size(); i++) {
            flagsReplies[i] = new BulkReply(flags.get(i).getBytes());
        }
        replies[1] = new MultiBulkReply(flagsReplies);

        replies[2] = new BulkReply("passwords".getBytes());
        var isOnlyNoPass = passwords.size() == 1 && passwords.getFirst().isNoPass();
        if (isOnlyNoPass || passwords.isEmpty()) {
            replies[3] = MultiBulkReply.EMPTY;
        } else {
            var passwordsReplies = new Reply[passwords.size()];
            for (int i = 0; i < passwords.size(); i++) {
                passwordsReplies[i] = new BulkReply(passwords.get(i).passwordEncoded.getBytes());
            }
            replies[3] = new MultiBulkReply(passwordsReplies);
        }

        replies[4] = new BulkReply("commands".getBytes());
        var commandsReplies = new Reply[rCmdList.size() + rCmdDisallowList.size()];
        for (int i = 0; i < rCmdList.size(); i++) {
            commandsReplies[i] = new BulkReply(rCmdList.get(i).literal().getBytes());
        }
        for (int i = 0; i < rCmdDisallowList.size(); i++) {
            commandsReplies[i + rCmdList.size()] = new BulkReply(rCmdDisallowList.get(i).literal().getBytes());
        }
        replies[5] = new MultiBulkReply(commandsReplies);

        replies[6] = new BulkReply("keys".getBytes());
        var keysReplies = new Reply[rKeyList.size()];
        for (int i = 0; i < rKeyList.size(); i++) {
            keysReplies[i] = new BulkReply(rKeyList.get(i).literal().getBytes());
        }
        replies[7] = new MultiBulkReply(keysReplies);

        replies[8] = new BulkReply("channels".getBytes());
        var channelsReplies = new Reply[rPubSubList.size()];
        for (int i = 0; i < rPubSubList.size(); i++) {
            channelsReplies[i] = new BulkReply(rPubSubList.get(i).literal().getBytes());
        }
        replies[9] = new MultiBulkReply(channelsReplies);

        return replies;
    }

    public static U fromLiteral(String str) {
        if (!str.startsWith("user")) {
            return null;
        }

        var parts = str.split(" ");
        if (parts.length < 4) {
            return null;
        }

        var user = parts[1];
        var isOn = "on".equals(parts[2]);
        var password = parts[3];

        var u = new U(user);
        u.setOn(isOn);
        if (Password.NO_PASS.equals(password)) {
            u.addPassword(Password.NO_PASSWORD);
        } else {
            // need trim # before password ? todo
            u.addPassword(Password.plain(password));
        }

        for (int i = 4; i < parts.length; i++) {
            var part = parts[i];
            if (part.startsWith("+")) {
                u.addRCmd(false, RCmd.fromLiteral(part));
            } else if (part.startsWith("-")) {
                u.addRCmdDisallow(false, RCmd.fromLiteral(part));
            } else if (part.startsWith("~") || part.startsWith("%")) {
                u.addRKey(false, RKey.fromLiteral(part));
            } else if (part.startsWith("&")) {
                u.addRPubSub(false, RPubSub.fromLiteral(part));
            } else {
                throw new IllegalArgumentException("Invalid literal: " + part);
            }
        }

        return u;
    }

    @VisibleForTesting
    final List<RCmd> rCmdList = new ArrayList<>();
    @VisibleForTesting
    final List<RCmd> rCmdDisallowList = new ArrayList<>();

    @VisibleForTesting
    final List<RKey> rKeyList = new ArrayList<>();

    @VisibleForTesting
    final List<RPubSub> rPubSubList = new ArrayList<>();

    public void resetCmd() {
        rCmdList.clear();
        rCmdDisallowList.clear();
    }

    public void resetKey() {
        rKeyList.clear();
    }

    public void resetPubSub() {
        rPubSubList.clear();
    }

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

    public void mergeRulesFromAnother(U another, boolean isReset) {
        if (isReset) {
            rCmdList.clear();
            rCmdDisallowList.clear();
            rKeyList.clear();
            rPubSubList.clear();
        }

        rCmdList.addAll(another.rCmdList);
        rCmdDisallowList.addAll(another.rCmdDisallowList);
        rKeyList.addAll(another.rKeyList);
        rPubSubList.addAll(another.rPubSubList);
    }

    public record CheckCmdAndKeyResult(boolean isOk, boolean isKeyFail) {
        public boolean asBoolean() {
            return isOk;
        }

        public static final CheckCmdAndKeyResult FALSE_WHEN_CHECK_CMD = new CheckCmdAndKeyResult(false, false);
        public static final CheckCmdAndKeyResult FALSE_WHEN_CHECK_KEY = new CheckCmdAndKeyResult(false, true);
        public static final CheckCmdAndKeyResult TRUE = new CheckCmdAndKeyResult(true, false);
    }

    public CheckCmdAndKeyResult checkCmdAndKey(String cmd, byte[][] data, ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList) {
        if (rCmdList.isEmpty()) {
            return FALSE_WHEN_CHECK_CMD;
        }

        var firstArg = data.length > 1 ? new String(data[1]).toLowerCase() : null;
        var isAllowCmdAnyOk = rCmdList.stream().anyMatch(rCmd -> rCmd.match(cmd, firstArg));
        if (!isAllowCmdAnyOk) {
            return FALSE_WHEN_CHECK_CMD;
        }

        var isDisallowCmdEveryOk = rCmdDisallowList.stream().noneMatch(rCmd -> rCmd.match(cmd, firstArg));
        if (!isDisallowCmdEveryOk) {
            return FALSE_WHEN_CHECK_CMD;
        }

        if (slotWithKeyHashList != null && !slotWithKeyHashList.isEmpty()) {
            if (rKeyList.isEmpty()) {
                // skip check, just use to fix event loop
                if (slotWithKeyHashList.size() == 1 && slotWithKeyHashList.getFirst() == BaseCommand.SlotWithKeyHash.TO_FIX_FIRST_SLOT) {
                    return CheckCmdAndKeyResult.TRUE;
                }
                return FALSE_WHEN_CHECK_KEY;
            }

            for (var slotWithKeyHash : slotWithKeyHashList) {
                if (slotWithKeyHash == BaseCommand.SlotWithKeyHash.TO_FIX_FIRST_SLOT) {
                    continue;
                }

                for (var rKey : rKeyList) {
                    // todo, cmd read / write, need to check
                    if (!rKey.match(slotWithKeyHash.rawKey(), true, true)) {
                        return FALSE_WHEN_CHECK_KEY;
                    }
                }
            }
        }

        return CheckCmdAndKeyResult.TRUE;
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
