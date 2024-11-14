package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.velo.BaseCommand;
import io.velo.acl.AclUsers;
import io.velo.acl.Category;
import io.velo.decode.Request;
import io.velo.reply.*;
import io.velo.util.Utils;
import org.jetbrains.annotations.VisibleForTesting;

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
            var keyBytes = data[1];
            var slotWithKeyHash = slot(keyBytes, slotNumber);
            slotWithKeyHashList.add(slotWithKeyHash);
            return slotWithKeyHashList;
        }

        if ("acl".equals(cmd)) {
            if (data.length > 2) {
                var isDryrun = "dryrun".equalsIgnoreCase(new String(data[2]));
                if (isDryrun) {
                    if (data.length < 4) {
                        return slotWithKeyHashList;
                    }

                    var dd = new byte[data.length - 3][];
                    System.arraycopy(data, 3, dd, 0, data.length - 3);

                    var redirectRequest = new Request(dd, false, false);
                    requestHandler.parseSlots(redirectRequest);
                    return redirectRequest.getSlotWithKeyHashList();
                } else {
                    // fix the first net worker event loop thread
                    slotWithKeyHashList.add(new SlotWithKeyHash((short) 0, 0, 0L));
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

            var aclUsers = AclUsers.getInstance();
            int count = 0;
            for (var user : userList) {
                if (aclUsers.delete(user)) {
                    count++;
                }
            }
            return new IntegerReply(count);
        } else if ("dryrun".equals(subCmd)) {
            if (data.length < 4) {
                return ErrorReply.SYNTAX;
            }

            var user = new String(data[2]);
            var aclUsers = AclUsers.getInstance();
            var u = aclUsers.get(user);
            if (u == null) {
                log.error("Acl dryrun no such user: {}", user);
                return ErrorReply.ACL_PERMIT_LIMIT;
            }

            if (!u.isOn()) {
                log.error("Acl dryrun user is off: {}", user);
                return ErrorReply.ACL_PERMIT_LIMIT;
            }

            var dd = new byte[data.length - 3][];
            System.arraycopy(data, 3, dd, 0, data.length - 3);

            var redirectRequest = new Request(dd, false, false);
            redirectRequest.setU(u);

            if (!redirectRequest.isAclCheckOk()) {
                return ErrorReply.ACL_PERMIT_LIMIT;
            }

            // todo

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
                return ErrorReply.INVALID_INTEGER;
            }

            if (givenNumber < 1 || givenNumber > 1024) {
                return new ErrorReply("Given number must be between 1 and 1024");
            }

            int len = givenNumber / 4;
            if (givenNumber % 4 != 0) {
                len++;
            }

            var randomChars = Utils.generateRandomChars(len);
            return new BulkReply(randomChars.getBytes());
        }

        // todo: implement other sub commands

        return ErrorReply.SYNTAX;
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
