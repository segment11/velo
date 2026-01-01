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

/**
 * Represents an ACL user in the Velo system.
 * This class encapsulates the properties and behaviors of a user, including:
 * - User status (enabled/disabled)
 * - User passwords
 * - Command and key access rules
 * - Channel subscriptions
 */
public class U {
    /**
     * The default user when no specific user is provided.
     */
    public static final String DEFAULT_USER = "default";

    /**
     * Prefix for adding a plain password.
     */
    public static final String ADD_PASSWORD_PREFIX = ">";
    /**
     * Prefix for removing a plain password.
     */
    public static final String REMOVE_PASSWORD_PREFIX = "<";
    /**
     * Prefix for adding a hashed password.
     */
    public static final String ADD_HASH_PASSWORD_PREFIX = "#";
    /**
     * Prefix for removing a hashed password.
     */
    public static final String REMOVE_HASH_PASSWORD_PREFIX = "!";

    /**
     * Enum representing the encoding type of a password.
     */
    private enum PasswordEncodedType {
        plain, sha256Hex;
    }

    /**
     * Record representing a user's password.
     */
    public record Password(String passwordEncoded, PasswordEncodedType encodeType) {
        /**
         * Constant representing no password.
         */
        public static final String NO_PASS = "nopass";

        /**
         * Sentinel Password instance representing no password.
         */
        public static final Password NO_PASSWORD = new Password(NO_PASS, PasswordEncodedType.plain);

        /**
         * Checks if the password is set to 'nopass'.
         *
         * @return true if the password is 'nopass', false otherwise.
         */
        @VisibleForTesting
        boolean isNoPass() {
            return NO_PASS.equals(this.passwordEncoded);
        }

        /**
         * Checks if the provided raw password matches this password.
         *
         * @param passwordRaw the raw password to check
         * @return true if the provided password matches, false otherwise
         */
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

        /**
         * Creates a plain password instance.
         *
         * @param passwordRaw the raw password
         * @return the Password instance with the plain encoding type
         */
        public static Password plain(String passwordRaw) {
            return new Password(passwordRaw, PasswordEncodedType.plain);
        }

        /**
         * Creates a SHA-256 hashed password instance.
         *
         * @param passwordRaw the raw password
         * @return the Password instance with the SHA-256 encoding type
         */
        public static Password sha256Hex(String passwordRaw) {
            return new Password(DigestUtils.sha256Hex(passwordRaw), PasswordEncodedType.sha256Hex);
        }

        /**
         * Creates a Password instance from a pre-encoded SHA-256 hash.
         *
         * @param passwordSha256Hex the pre-encoded SHA-256 hash
         * @return the Password instance with the SHA-256 encoding type
         */
        public static Password sha256HexEncoded(String passwordSha256Hex) {
            return new Password(passwordSha256Hex, PasswordEncodedType.sha256Hex);
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
    }

    /**
     * The username.
     */
    final String user;

    /**
     * Constructs a new user with the given username.
     *
     * @param user the username
     */
    public U(String user) {
        this.user = user;
    }

    /**
     * Returns the username.
     *
     * @return the username
     */
    public String getUser() {
        return user;
    }

    /**
     * Whether the user is enabled.
     */
    private boolean isOn = false;

    /**
     * Returns whether the user is enabled.
     *
     * @return true if the user is enabled, false otherwise.
     */
    public boolean isOn() {
        return isOn;
    }

    /**
     * Sets the user's enabled status.
     *
     * @param on true to enable the user, false to disable the user
     */
    public void setOn(boolean on) {
        isOn = on;
    }

    /**
     * List of passwords for the user.
     */
    private final ArrayList<Password> passwords = new ArrayList<>();

    /**
     * Adds a password to the user.
     *
     * @param password the password to add
     */
    public void addPassword(Password password) {
        // check duplicate
        if (passwords.stream().anyMatch(p -> p.equals(password))) {
            return;
        }
        passwords.add(password);
    }

    /**
     * Removes a password from the user.
     *
     * @param password the password to remove
     */
    public void removePassword(Password password) {
        passwords.stream().filter(p -> p.equals(password)).findFirst().ifPresent(passwords::remove);
    }

    /**
     * Resets (clears) all passwords for the user.
     */
    public void resetPassword() {
        passwords.clear();
    }

    /**
     * Checks if the provided raw password matches any of the user's passwords.
     *
     * @param passwordRaw the raw password to check
     * @return true if the password matches, false otherwise
     */
    public boolean checkPassword(String passwordRaw) {
        // reset password
        if (passwords.isEmpty()) {
            return false;
        }
        return passwords.stream().anyMatch(password -> password.check(passwordRaw));
    }

    /**
     * Sets the user's password. Clears any existing passwords.
     *
     * @param password the password to set
     */
    @TestOnly
    public void setPassword(Password password) {
        passwords.clear();
        passwords.add(password);
    }

    /**
     * Returns the first password of the user.
     *
     * @return the first password, or null if there are no passwords
     */
    @TestOnly
    public Password getFirstPassword() {
        return passwords.isEmpty() ? null : passwords.getFirst();
    }

    /**
     * Default initialized user with 'default' username.
     */
    public static final U INIT_DEFAULT_U = new U(DEFAULT_USER);

    static {
        INIT_DEFAULT_U.setOn(true);
        INIT_DEFAULT_U.addPassword(Password.NO_PASSWORD);
        INIT_DEFAULT_U.addRCmd(true, RCmd.fromLiteral("+*"), RCmd.fromLiteral("+@all"));
        INIT_DEFAULT_U.addRKey(true, RKey.fromLiteral("~*"));
        INIT_DEFAULT_U.addRPubSub(true, RPubSub.fromLiteral("&*"));
    }

    /**
     * Returns a string representation of the user in a literal format.
     *
     * @return the string representation of the user
     */
    public String literal() {
        var sb = new StringBuilder();
        sb.append("user ").append(user).append(" ");
        sb.append(isOn ? "on" : "off").append(" ");
        var firstPassword = getFirstPassword();
        if (firstPassword != null) {
            if (firstPassword.encodeType == PasswordEncodedType.sha256Hex) {
                sb.append(ADD_HASH_PASSWORD_PREFIX).append(firstPassword.passwordEncoded).append(" ");
            } else {
                sb.append(firstPassword.passwordEncoded).append(" ");
            }
        }

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
        if (!sb.isEmpty()) {
            sb.deleteCharAt(sb.length() - 1);
        }
        return sb.toString();
    }

    /**
     * Returns an array of Redis replies representing the user's properties.
     *
     * @return the array of Redis replies representing the user's properties
     */
    public Reply[] toReplies() {
        var replies = new Reply[10];
        replies[0] = new BulkReply("flags");

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
            flagsReplies[i] = new BulkReply(flags.get(i));
        }
        replies[1] = new MultiBulkReply(flagsReplies);

        replies[2] = new BulkReply("passwords");
        var isOnlyNoPass = passwords.size() == 1 && passwords.getFirst().isNoPass();
        if (isOnlyNoPass || passwords.isEmpty()) {
            replies[3] = MultiBulkReply.EMPTY;
        } else {
            var passwordsReplies = new Reply[passwords.size()];
            for (int i = 0; i < passwords.size(); i++) {
                passwordsReplies[i] = new BulkReply(passwords.get(i).passwordEncoded);
            }
            replies[3] = new MultiBulkReply(passwordsReplies);
        }

        replies[4] = new BulkReply("commands");
        var commandsReplies = new Reply[rCmdList.size() + rCmdDisallowList.size()];
        for (int i = 0; i < rCmdList.size(); i++) {
            commandsReplies[i] = new BulkReply(rCmdList.get(i).literal());
        }
        for (int i = 0; i < rCmdDisallowList.size(); i++) {
            commandsReplies[i + rCmdList.size()] = new BulkReply(rCmdDisallowList.get(i).literal());
        }
        replies[5] = new MultiBulkReply(commandsReplies);

        replies[6] = new BulkReply("keys");
        var keysReplies = new Reply[rKeyList.size()];
        for (int i = 0; i < rKeyList.size(); i++) {
            keysReplies[i] = new BulkReply(rKeyList.get(i).literal());
        }
        replies[7] = new MultiBulkReply(keysReplies);

        replies[8] = new BulkReply("channels");
        var channelsReplies = new Reply[rPubSubList.size()];
        for (int i = 0; i < rPubSubList.size(); i++) {
            channelsReplies[i] = new BulkReply(rPubSubList.get(i).literal());
        }
        replies[9] = new MultiBulkReply(channelsReplies);

        return replies;
    }

    /**
     * Creates a user instance from a literal string representation.
     *
     * @param str the literal string representation of the user
     * @return the user instance, or null if the string is invalid
     */
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
        } else if (password.startsWith(ADD_HASH_PASSWORD_PREFIX)) {
            u.addPassword(Password.sha256HexEncoded(password.substring(1)));
        } else {
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

    /**
     * List of allowed commands for the user.
     */
    @VisibleForTesting
    final List<RCmd> rCmdList = new ArrayList<>();
    /**
     * List of disallowed commands for the user.
     */
    @VisibleForTesting
    final List<RCmd> rCmdDisallowList = new ArrayList<>();

    /**
     * List of allowed keys for the user.
     */
    @VisibleForTesting
    final List<RKey> rKeyList = new ArrayList<>();

    /**
     * List of allowed channels for the user.
     */
    @VisibleForTesting
    final List<RPubSub> rPubSubList = new ArrayList<>();

    /**
     * Resets the command rules for the user.
     */
    public void resetCmd() {
        rCmdList.clear();
        rCmdDisallowList.clear();
    }

    /**
     * Resets the key rules for the user.
     */
    public void resetKey() {
        rKeyList.clear();
    }

    /**
     * Resets the channel rules for the user.
     */
    public void resetPubSub() {
        rPubSubList.clear();
    }

    /**
     * Adds command rules to the user.
     *
     * @param clear if true, clears existing command rules before adding
     * @param rCmd  the command rules to add
     */
    public void addRCmd(boolean clear, RCmd... rCmd) {
        if (clear) {
            rCmdList.clear();
        }
        rCmdList.addAll(Arrays.asList(rCmd));
    }

    /**
     * Adds disallowed command rules to the user.
     *
     * @param clear if true, clears existing disallowed command rules before adding
     * @param rCmd  the disallowed command rules to add
     */
    public void addRCmdDisallow(boolean clear, RCmd... rCmd) {
        if (clear) {
            rCmdDisallowList.clear();
        }
        rCmdDisallowList.addAll(Arrays.asList(rCmd));
    }

    /**
     * Adds key rules to the user.
     *
     * @param clear if true, clears existing key rules before adding
     * @param rKey  the key rules to add
     */
    public void addRKey(boolean clear, RKey... rKey) {
        if (clear) {
            rKeyList.clear();
        }
        rKeyList.addAll(Arrays.asList(rKey));
    }

    /**
     * Adds channel rules to the user.
     *
     * @param clear   if true, clears existing channel rules before adding
     * @param rPubSub the channel rules to add
     */
    public void addRPubSub(boolean clear, RPubSub... rPubSub) {
        if (clear) {
            rPubSubList.clear();
        }
        rPubSubList.addAll(Arrays.asList(rPubSub));
    }

    /**
     * Merges rules from another user into this user.
     *
     * @param another the user whose rules are to be merged
     * @param isReset if true, resets this user's rules before merging
     */
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

    /**
     * Access check result when checking command and key access.
     *
     * @param isOk      whether the result is OK
     * @param isKeyFail whether the result is due to a key failure; when false means it is due to a command failure
     */
    public record CheckCmdAndKeyResult(boolean isOk, boolean isKeyFail) {
        public boolean asBoolean() {
            return isOk;
        }

        public static final CheckCmdAndKeyResult FALSE_WHEN_CHECK_CMD = new CheckCmdAndKeyResult(false, false);
        public static final CheckCmdAndKeyResult FALSE_WHEN_CHECK_KEY = new CheckCmdAndKeyResult(false, true);
        public static final CheckCmdAndKeyResult TRUE = new CheckCmdAndKeyResult(true, false);
    }

    /**
     * Checks if the user has access to the given command and key.
     *
     * @param cmd                 the command
     * @param data                the command arguments
     * @param slotWithKeyHashList the keys
     * @return the CheckCmdAndKeyResult instance representing the access check result
     */
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

    /**
     * Checks if the user has access to the given channels.
     *
     * @param channels the channels
     * @return true if the user has access to all the channels, false otherwise
     */
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
