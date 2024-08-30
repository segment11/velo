package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ToSlaveExistsBigString implements ReplContent {
    private final File bigStringDir;
    private final List<Long> toSendUuidList;
    private final boolean isSendAllOnce;

    static final int ONCE_SEND_BIG_STRING_COUNT = 10;

    public ToSlaveExistsBigString(File bigStringDir, List<Long> uuidListInMaster, List<Long> sentUuidList) {
        this.bigStringDir = bigStringDir;

        var toSendUuidList = new ArrayList<Long>();
        // exclude sent uuid
        for (var uuid : uuidListInMaster) {
            if (!sentUuidList.contains(uuid)) {
                toSendUuidList.add(uuid);
            }
        }

        this.isSendAllOnce = toSendUuidList.size() <= ONCE_SEND_BIG_STRING_COUNT;
        if (!isSendAllOnce) {
            toSendUuidList = new ArrayList<>(toSendUuidList.subList(0, ONCE_SEND_BIG_STRING_COUNT));
        }

        this.toSendUuidList = toSendUuidList;
    }

    // 4 bytes as int for send big string count, 1 byte as flag for is sent all
    private static final int HEADER_LENGTH = 4 + 1;

    @Override
    public void encodeTo(ByteBuf toBuf) {
        if (toSendUuidList.isEmpty()) {
            toBuf.writeInt(0);
            toBuf.writeByte((byte) 1);
            return;
        }

        toBuf.writeInt(toSendUuidList.size());
        toBuf.writeByte((byte) (isSendAllOnce ? 1 : 0));

        var existCount = 0;
        for (var uuid : toSendUuidList) {
            var file = new File(bigStringDir, String.valueOf(uuid));
            if (!file.exists()) {
                continue;
            }

            existCount++;

            toBuf.writeLong(uuid);
            var bigStringBytesLength = (int) file.length();
            toBuf.writeInt(bigStringBytesLength);

            try {
                byte[] bytes = FileUtils.readFileToByteArray(file);
                toBuf.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (existCount != toSendUuidList.size()) {
            toBuf.tail(0);
            toBuf.writeInt(existCount);
        }
    }

    @Override
    public int encodeLength() {
        if (toSendUuidList.isEmpty()) {
            return HEADER_LENGTH;
        }

        var length = HEADER_LENGTH;
        for (var uuid : toSendUuidList) {
            var file = new File(bigStringDir, String.valueOf(uuid));
            if (!file.exists()) {
                continue;
            }

            var bigStringBytesLength = (int) file.length();
            length += 8 + 4 + bigStringBytesLength;
        }
        return length;
    }
}
