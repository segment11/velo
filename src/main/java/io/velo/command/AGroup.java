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
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class AGroup extends BaseCommand {
    public AGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

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

    @Override
    public Reply handle() {
        if ("acl".equals(cmd)) {
            return acl();
        }

        if ("append".equals(cmd)) {
            return append();
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
                    replies[i] = new BulkReply(categories[i].name().getBytes());
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
                    replies[i] = new BulkReply(cmdList.get(i).getBytes());
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

            var isAclCheckOk = redirectRequest.isAclCheckOk();
            if (!isAclCheckOk.asBoolean()) {
                return isAclCheckOk.isKeyFail() ? ErrorReply.ACL_PERMIT_KEY_LIMIT : ErrorReply.ACL_PERMIT_LIMIT;
            }

            redirectRequest.setSlotWithKeyHashList(slotWithKeyHashListParsed);
            return requestHandler.handle(redirectRequest, socket);
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
            return new BulkReply(randomChars.getBytes());
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

            var users = aclUsers.getInner().getUsers();

            var replies = new Reply[users.size()];
            for (int i = 0; i < users.size(); i++) {
                replies[i] = new BulkReply(users.get(i).literal().getBytes());
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

            List<U> users = new ArrayList<>();
            try {
                var lines = FileUtils.readLines(aclFile, "UTF-8");
                for (var line : lines) {
                    if (line.startsWith("#")) {
                        continue;
                    }

                    var u = U.fromLiteral(line);
                    if (u == null) {
                        return new ErrorReply("parse acl file error: " + line);
                    }
                    users.add(u);
                }
            } catch (IOException e) {
                log.error("Read acl file error={}", e.getMessage());
                return new ErrorReply("read acl file error: " + e.getMessage());
            } catch (IllegalArgumentException e) {
                log.error("Parse acl file error={}", e.getMessage());
                return new ErrorReply("parse acl file error: " + e.getMessage());
            }

            // must include default user
            if (users.stream().noneMatch(u -> u.getUser().equals(U.INIT_DEFAULT_U.getUser()))) {
                return new ErrorReply("no default user in acl file");
            }

            aclUsers.replaceUsers(users);
            return OKReply.INSTANCE;
        } else if ("log".equals(subCmd)) {
            if (data.length != 3) {
                return ErrorReply.SYNTAX;
            }

            var countOrReset = new String(data[2]).toLowerCase();
            if ("reset".equals(countOrReset)) {
                // todo, clear log rows
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
                    return new ErrorReply("count must be between 1 and 1000");
                }

                var topReplies = new Reply[count];
                for (int i = 0; i < count; i++) {
                    // todo, get log row
                    var logRow = new LogRow();
                    logRow.count = count;
                    logRow.reason = "reason";
                    logRow.context = "context";
                    logRow.object = "object";
                    logRow.username = "username";
                    logRow.ageSeconds = 1.0;
                    logRow.clientInfo = "client-info";
                    logRow.entryId = 1;
                    logRow.timestampCreated = 1;
                    logRow.timestampLastUpdated = 1;

                    var replies = logRow.toReplies();
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

            var users = aclUsers.getInner().getUsers();
            var lines = new ArrayList<String>();
            for (var u : users) {
                lines.add(u.literal());
            }

            try {
                FileUtils.writeLines(aclFile, "UTF-8", lines);

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

            try {
                aclUsers.upInsert(user, u -> {
                    for (int i = 3; i < data.length; i++) {
                        var rule = new String(data[i]);
                        if ("on".equals(rule)) {
                            u.setOn(true);
                        } else if ("off".equals(rule)) {
                            u.setOn(false);
                        } else if ("resetkeys".equals(rule)) {
                            u.resetKey();
                        } else if ("resetchannels".equals(rule)) {
                            u.resetPubSub();
                            if (ValkeyRawConfSupport.aclPubsubDefault) {
                                u.addRPubSub(false, RPubSub.fromLiteral("&*"));
                            }
                        } else if ("nopass".equals(rule)) {
                            u.addPassword(U.Password.NO_PASSWORD);
                        } else if ("resetpass".equals(rule)) {
                            u.resetPassword();
                        } else if (rule.startsWith(U.ADD_PASSWORD_PREFIX)) {
                            var password = rule.substring(1);
                            u.addPassword(U.Password.plain(password));
                        } else if (rule.startsWith(U.REMOVE_PASSWORD_PREFIX)) {
                            var password = rule.substring(1);
                            u.removePassword(U.Password.plain(password));
                        } else if (rule.startsWith(U.ADD_HASH_PASSWORD_PREFIX)) {
                            var passwordSha256Hex = rule.substring(1);
                            if (passwordSha256Hex.length() != 64) {
                                throw new AclInvalidRuleException("Invalid sha256 hex password: " + passwordSha256Hex);
                            }
                            u.addPassword(U.Password.sha256HexEncoded(passwordSha256Hex));
                        } else if (rule.startsWith(U.REMOVE_HASH_PASSWORD_PREFIX)) {
                            var passwordSha256Hex = rule.substring(1);
                            if (passwordSha256Hex.length() != 64) {
                                throw new AclInvalidRuleException("Invalid sha256 hex password: " + passwordSha256Hex);
                            }
                            u.removePassword(U.Password.sha256HexEncoded(passwordSha256Hex));
                        } else if ("reset".equals(rule)) {
                            u.setOn(false);
                            u.resetPassword();
                            u.resetKey();
                            u.resetPubSub();
                            if (ValkeyRawConfSupport.aclPubsubDefault) {
                                u.addRPubSub(false, RPubSub.fromLiteral("&*"));
                            }
                        } else if (RCmd.isRCmdLiteral(rule)) {
                            if (RCmd.isAllowLiteral(rule)) {
                                u.addRCmd(false, RCmd.fromLiteral(rule));
                            } else {
                                u.addRCmdDisallow(false, RCmd.fromLiteral(rule));
                            }
                        } else if (RKey.isRKeyLiteral(rule)) {
                            u.addRKey(false, RKey.fromLiteral(rule));
                        } else if (RPubSub.isRPubSubLiteral(rule)) {
                            u.addRPubSub(false, RPubSub.fromLiteral(rule));
                        } else {
                            throw new IllegalArgumentException("Invalid acl rule: " + rule);
                        }
                    }
                });

                appendAclUpdateBinlog();
                return OKReply.INSTANCE;
            } catch (AclInvalidRuleException e) {
                log.error("Set user error={}", e.getMessage());
                return ErrorReply.ACL_SETUSER_RULE_INVALID;
            }
        } else if ("users".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.SYNTAX;
            }

            var users = aclUsers.getInner().getUsers();

            var replies = new Reply[users.size()];
            for (int i = 0; i < users.size(); i++) {
                replies[i] = new BulkReply(users.get(i).getUser().getBytes());
            }
            return new MultiBulkReply(replies);
        } else if ("whoami".equals(subCmd)) {
            if (data.length != 2) {
                return ErrorReply.SYNTAX;
            }

            var u = getAuthU();
            return new BulkReply(u.getUser().getBytes());
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

        var keyBytes = data[1];
        var valueBytes = data[2];

        int length;

        var slotWithKeyHash = slotWithKeyHashListParsed.getFirst();
        var existsValueBytes = get(keyBytes, slotWithKeyHash);
        if (existsValueBytes == null) {
            set(keyBytes, valueBytes, slotWithKeyHash);
            length = valueBytes.length;
        } else {
            var newValueBytes = new byte[existsValueBytes.length + valueBytes.length];
            System.arraycopy(existsValueBytes, 0, newValueBytes, 0, existsValueBytes.length);
            System.arraycopy(valueBytes, 0, newValueBytes, existsValueBytes.length, valueBytes.length);

            set(keyBytes, newValueBytes, slotWithKeyHash);
            length = newValueBytes.length;
        }

        return new IntegerReply(length);
    }
}
