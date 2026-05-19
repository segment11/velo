package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.Utils;
import io.velo.ValkeyRawConfSupport;
import io.velo.acl.*;
import io.velo.decode.Request;
import io.velo.repl.incremental.XAclUpdate;
import io.velo.reply.*;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toSet;

/**
 * Handles Redis commands starting with letter 'A'.
 * This includes commands like ACL, APPEND, ASKING, etc.
 */
public class AGroup extends BaseCommand {
    /**
     * @param cmd    the command string
     * @param data   the data array
     * @param socket the TCP socket
     */
    public AGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    /**
     * Parses slot information from the command.
     *
     * @param cmd        the command name
     * @param data       the command arguments
     * @param slotNumber current slot number
     * @return list containing slot with key hash, or empty list
     */
    @Override
    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if ("append".equals(cmd)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }
            slotWithKeyHashList.add(slot(data[1], slotNumber));
            return slotWithKeyHashList;
        }

        if ("acl".equals(cmd)) {
            if (data.length > 2) {
                var isDryrun = "dryrun".equalsIgnoreCase(new String(data[1]));
                if (isDryrun) {
                    if (data.length < 4) {
                        return slotWithKeyHashList;
                    }

                    var dd = new byte[data.length - 3][];
                    System.arraycopy(data, 3, dd, 0, data.length - 3);

                    var redirectRequest = new Request(dd, false, false);
                    redirectRequest.setSlotNumber((short) slotNumber);
                    requestHandler.parseSlots(redirectRequest);
                    return redirectRequest.getSlotWithKeyHashList();
                } else {
                    slotWithKeyHashList.add(SlotWithKeyHash.TO_FIX_FIRST_SLOT);
                }
            }
        }

        return slotWithKeyHashList;
    }

    /**
     * Handles the command and returns the reply.
     *
     * @return the reply for this command
     */
    @Override
    public Reply handle() {
        if ("acl".equals(cmd)) {
            return acl();
        }

        if ("append".equals(cmd)) {
            return append();
        }

        if ("asking".equals(cmd)) {
            // velo slot migration node A to node B, not by keys like redis, end when B all received.
            return OKReply.INSTANCE;
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply acl() {
        if (data.length < 2) {
            return ErrorReply.FORMAT;
        }

        var subCmd = new String(data[1]).toLowerCase();

        if ("cat".equals(subCmd)) {
            if (data.length != 2 && data.length != 3) {
                return ErrorReply.SYNTAX;
            }

            var category = data.length == 3 ? new String(data[2]).toLowerCase() : null;
            var categories = Category.values();
            if (category == null) {
                var replies = new Reply[categories.length];
                for (int i = 0; i < categories.length; i++) {
                    replies[i] = new BulkReply(categories[i].name());
                }
                return new MultiBulkReply(replies);
            } else {
                int index = -1;
                for (int i = 0; i < categories.length; i++) {
                    if (categories[i].name().equals(category)) {
                        index = i;
                        break;
                    }
                }
                if (index == -1) {
                    return ErrorReply.SYNTAX;
                }

                var cmdList = Category.getCmdListByCategory(categories[index]);
                var replies = new Reply[cmdList.size()];
                for (int i = 0; i < cmdList.size(); i++) {
                    replies[i] = new BulkReply(cmdList.get(i));
                }
                return new MultiBulkReply(replies);
            }
        } else if ("deluser".equals(subCmd)) {
            if (data.length < 3) {
                return ErrorReply.SYNTAX;
            }

            List<String> userList = new ArrayList<>();
            for (int i = 2; i < data.length; i++) {
                var user = new String(data[i]);
                userList.add(user);
            }

            int count = 0;
            for (var user : userList) {
                if (user.equals(U.DEFAULT_USER)) {
                    continue;
                }
                if (aclUsers.delete(user)) {
                    count++;
                }
            }

            appendAclUpdateBinlog();
            return new IntegerReply(count);
        } else if ("dryrun".equals(subCmd)) {
            if (data.length < 4) {
                return ErrorReply.SYNTAX;
            }

            var user = new String(data[2]);
            var u = aclUsers.get(user);
            if (u == null) {
                log.error("Acl dryrun no such user={}", user);
                return ErrorReply.ACL_PERMIT_LIMIT;
            }

            if (!u.isOn()) {
                log.error("Acl dryrun user is off={}", user);
                return ErrorReply.ACL_PERMIT_LIMIT;
            }

            var dd = new byte[data.length - 3][];
            System.arraycopy(data, 3, dd, 0, data.length - 3);

            var redirectRequest = new Request(dd, false, false);
            redirectRequest.setU(u);
            redirectRequest.setSlotWithKeyHashList(slotWithKeyHashListParsed);

            var aclCheckResult = redirectRequest.isAclCheckOk();
            if (!aclCheckResult.asBoolean()) {
                return aclCheckResult.isKeyFail() ? ErrorReply.ACL_PERMIT_KEY_LIMIT : ErrorReply.ACL_PERMIT_LIMIT;
            }

            return OKReply.INSTANCE;
        } else if ("genpass".equals(subCmd)) {
            if (data.length != 2 && data.length != 3) {
                return ErrorReply.SYNTAX;
            }

            var givenNumberString = data.length == 3 ? new String(data[2]) : "256";
            int givenNumber;
            try {
                givenNumber = Integer.parseInt(givenNumberString);
            } catch (NumberFormatException e) {
                return ErrorReply.NOT_INTEGER;
            }

            if (givenNumber < 1 || givenNumber > 1024) {
                return new ErrorReply("given number must be between 1 and 1024");
            }

            int len = givenNumber / 4;
            if (givenNumber % 4 != 0) {
                len++;
            }

            var randomChars = Utils.generateRandomChars(len);
            return new BulkReply(randomChars);
        } else if ("getuser".equals(subCmd)) {
            if (data.length != 3) {
                return ErrorReply.SYNTAX;
            }

            var user = new String(data[2]);
            var u = aclUsers.get(user);
            if (u == null) {
                return NilReply.INSTANCE;
            }

            return new MultiBulkReply(u.toReplies());
        } else if ("list".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.SYNTAX;
            }

            var uList = aclUsers.getUList();

            var replies = new Reply[uList.size()];
            for (int i = 0; i < uList.size(); i++) {
                replies[i] = new BulkReply(uList.get(i).literal());
            }
            return new MultiBulkReply(replies);
        } else if ("load".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.SYNTAX;
            }

            var aclFile = Paths.get(ValkeyRawConfSupport.aclFilename).toFile();
            if (!aclFile.exists()) {
                return ErrorReply.NO_SUCH_FILE;
            }

            try {
                // capture users before load to compute deletions
                var previousUList = aclUsers.getInner().getUList();

                aclUsers.loadAclFile();

                // replicate loaded users to replicas
                var firstOneSlot = localPersist.firstOneSlot();
                if (firstOneSlot != null && firstOneSlot.getDynConfig().isBinlogOn()) {
                    var uList = aclUsers.getUList();
                    var loadedUserNames = uList.stream()
                            .map(U::getUser)
                            .collect(toSet());

                    var lines = new ArrayList<String>();

                    // replicate deletions for users removed by the load
                    for (var prevU : previousUList) {
                        if (!loadedUserNames.contains(prevU.getUser())
                                && !prevU.getUser().equals(U.DEFAULT_USER)) {
                            lines.add("acl deluser " + prevU.getUser());
                        }
                    }

                    // replicate loaded users
                    for (var u : uList) {
                        var setuserLine = "acl setuser " + u.literal().substring("user ".length());
                        lines.add(setuserLine);
                    }

                    firstOneSlot.asyncCall(() -> {
                        try {
                            firstOneSlot.getBinlog().append(new XAclUpdate(lines));
                        } catch (IOException e) {
                            throw new RuntimeException("Append binlog error, acl lines=" + lines, e);
                        }
                        return null;
                    });
                }

                return OKReply.INSTANCE;
            } catch (Exception e) {
                return new ErrorReply(e.getMessage());
            }
        } else if ("log".equals(subCmd)) {
            if (data.length != 3) {
                return ErrorReply.SYNTAX;
            }

            var countOrReset = new String(data[2]).toLowerCase();
            if ("reset".equals(countOrReset)) {
                AclUsers.resetAclLogs();
                return OKReply.INSTANCE;
            } else {
                int count;
                try {
                    count = Integer.parseInt(countOrReset);
                } catch (NumberFormatException e) {
                    return ErrorReply.NOT_INTEGER;
                }

                // limit count
                if (count < 1 || count > 100) {
                    return new ErrorReply("count must be between 1 and 100");
                }

                var logRows = AclUsers.getAclLogs(count);
                var topReplies = new Reply[logRows.length];
                for (int i = 0; i < logRows.length; i++) {
                    var replies = logRows[i].toReplies();
                    topReplies[i] = new MultiBulkReply(replies);
                }
                return new MultiBulkReply(topReplies);
            }
        } else if ("save".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.SYNTAX;
            }

            var aclFile = Paths.get(ValkeyRawConfSupport.aclFilename).toFile();
            if (!aclFile.exists()) {
                try {
                    FileUtils.touch(aclFile);
                } catch (IOException e) {
                    return new ErrorReply("create acl file error: " + e.getMessage());
                }
            }

            var uList = aclUsers.getUList();
            var lines = new ArrayList<String>();
            for (var u : uList) {
                lines.add(u.literal());
            }

            try {
                var tmpFile = new java.io.File(aclFile.getParent(), aclFile.getName() + ".tmp");
                FileUtils.writeLines(tmpFile, "UTF-8", lines);
                Files.move(tmpFile.toPath(), aclFile.toPath(), StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);

                appendAclUpdateBinlog();
                return OKReply.INSTANCE;
            } catch (IOException e) {
                return new ErrorReply("write acl file error: " + e.getMessage());
            }
        } else if ("setuser".equals(subCmd)) {
            if (data.length < 3) {
                return ErrorReply.SYNTAX;
            }

            var user = new String(data[2]);
            var pubsubDefault = ValkeyRawConfSupport.aclPubsubDefault;

            try {
                aclUsers.upInsert(user, u -> {
                    var temp = new U(user);
                    temp.copyStateFrom(u);

                    for (int i = 3; i < data.length; i++) {
                        var rule = new String(data[i]);
                        if ("on".equals(rule)) {
                            temp.setOn(true);
                        } else if ("off".equals(rule)) {
                            temp.setOn(false);
                        } else if ("resetkeys".equals(rule)) {
                            temp.resetKey();
                        } else if ("resetchannels".equals(rule)) {
                            temp.resetPubSub();
                            if (pubsubDefault) {
                                temp.addRPubSub(false, RPubSub.fromLiteral("&*"));
                            }
                        } else if ("nopass".equals(rule)) {
                            temp.addPassword(U.Password.NO_PASSWORD);
                        } else if ("resetpass".equals(rule)) {
                            temp.resetPassword();
                        } else if (rule.startsWith(U.ADD_PASSWORD_PREFIX)) {
                            var password = rule.substring(1);
                            temp.addPassword(U.Password.plain(password));
                        } else if (rule.startsWith(U.REMOVE_PASSWORD_PREFIX)) {
                            var password = rule.substring(1);
                            temp.removePassword(U.Password.plain(password));
                        } else if (rule.startsWith(U.ADD_HASH_PASSWORD_PREFIX)) {
                            var passwordSha256Hex = rule.substring(1);
                            if (passwordSha256Hex.length() != 64) {
                                throw new AclInvalidRuleException("Invalid sha256 hex password: " + passwordSha256Hex);
                            }
                            temp.addPassword(U.Password.sha256HexEncoded(passwordSha256Hex));
                        } else if (rule.startsWith(U.REMOVE_HASH_PASSWORD_PREFIX)) {
                            var passwordSha256Hex = rule.substring(1);
                            if (passwordSha256Hex.length() != 64) {
                                throw new AclInvalidRuleException("Invalid sha256 hex password: " + passwordSha256Hex);
                            }
                            temp.removePassword(U.Password.sha256HexEncoded(passwordSha256Hex));
                        } else if ("reset".equals(rule)) {
                            temp.setOn(false);
                            temp.resetPassword();
                            temp.resetCmd();
                            temp.resetKey();
                            temp.resetPubSub();
                            if (pubsubDefault) {
                                temp.addRPubSub(false, RPubSub.fromLiteral("&*"));
                            }
                        } else if (RCmd.isRCmdLiteral(rule)) {
                            if (RCmd.isAllowLiteral(rule)) {
                                temp.addRCmd(false, RCmd.fromLiteral(rule));
                            } else {
                                temp.addRCmdDisallow(false, RCmd.fromLiteral(rule));
                            }
                        } else if (RKey.isRKeyLiteral(rule)) {
                            temp.addRKey(false, RKey.fromLiteral(rule));
                        } else if (RPubSub.isRPubSubLiteral(rule)) {
                            temp.addRPubSub(false, RPubSub.fromLiteral(rule));
                        } else {
                            throw new IllegalArgumentException("Invalid acl rule: " + rule);
                        }
                    }

                    u.copyStateFrom(temp);
                });

                appendAclUpdateBinlog();
                return OKReply.INSTANCE;
            } catch (AclInvalidRuleException | IllegalArgumentException e) {
                log.error("Set user error={}", e.getMessage());
                return ErrorReply.ACL_SETUSER_RULE_INVALID;
            }
        } else if ("users".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.SYNTAX;
            }

            var uList = aclUsers.getUList();

            var replies = new Reply[uList.size()];
            for (int i = 0; i < uList.size(); i++) {
                replies[i] = new BulkReply(uList.get(i).getUser());
            }
            return new MultiBulkReply(replies);
        } else if ("whoami".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.SYNTAX;
            }

            var u = getAuthU();
            if (u == null) {
                return ErrorReply.NO_AUTH;
            }
            return new BulkReply(u.getUser());
        }

        return ErrorReply.SYNTAX;
    }

    private void appendAclUpdateBinlog() {
        var line = dataToLine();
        var firstOneSlot = localPersist.firstOneSlot();
        if (firstOneSlot != null && firstOneSlot.getDynConfig().isBinlogOn()) {
            var p = firstOneSlot.asyncCall(() -> {
                try {
                    firstOneSlot.getBinlog().append(new XAclUpdate(line));
                    return true;
                } catch (IOException e) {
                    throw new RuntimeException("Append binlog error, acl line=" + line, e);
                }
            });
            // sync
            var r = p.getResult();
            var e = p.getException();
            if (e != null) {
                throw (RuntimeException) e;
            }
            log.warn("Append binlog success, acl line={}, result={}", line, r);
        }
    }

    @VisibleForTesting
    Reply append() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var valueBytes = data[2];

        int length;

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var existsValueBytes = get(slotWithKeyHash);
        if (existsValueBytes == null) {
            set(valueBytes, slotWithKeyHash);
            length = valueBytes.length;
        } else {
            var newValueBytes = new byte[existsValueBytes.length + valueBytes.length];
            System.arraycopy(existsValueBytes, 0, newValueBytes, 0, existsValueBytes.length);
            System.arraycopy(valueBytes, 0, newValueBytes, existsValueBytes.length, valueBytes.length);

            set(newValueBytes, slotWithKeyHash);
            length = newValueBytes.length;
        }

        return new IntegerReply(length);
    }
}
