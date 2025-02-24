package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Represents the content of a REPL message of type {@link io.velo.repl.ReplType#s_exists_big_string}.
 * This message is used to send information about big strings from a master to a slave.
 * It includes a list of UUIDs of big strings that need to be sent and whether all of them are being sent in one batch.
 */
public class ToSlaveExistsBigString implements ReplContent {
    private final File bigStringDir;
    private final List<Long> toSendUuidList;
    private final boolean isSendAllOnce;

    /**
     * The maximum number of big strings that can be sent in one batch.
     */
    static final int ONCE_SEND_BIG_STRING_COUNT = 10;

    /**
     * Constructs a new {@link ToSlaveExistsBigString} message.
     *
     * @param bigStringDir     The directory containing the big strings (files).
     * @param uuidListInMaster The list of UUIDs of big strings in the master.
     * @param sentUuidList     The list of UUIDs of big strings that have already been sent to the slave.
     */
    public ToSlaveExistsBigString(File bigStringDir, List<Long> uuidListInMaster, List<Long> sentUuidList) {
        this.bigStringDir = bigStringDir;

        var toSendUuidList = new ArrayList<Long>();
        // Exclude UUIDs that have already been sent
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

    /**
     * The length of the header in the encoded message, which includes:
     * - 4 bytes for the(send big string count).
     * - 1 byte as a flag indicating whether all big strings are sent in one batch.
     */
    private static final int HEADER_LENGTH = 4 + 1;

    /**
     * Encodes the content of this message to the given {@link ByteBuf}.
     * <p>
     * The encoding format is as follows:
     * - First 4 bytes: the count of big strings to send.
     * - Next 1 byte: a flag indicating if all big strings are sent in one batch (1 for true, 0 for false).
     * - For each big string to send:
     * - 8 bytes: the UUID of the big string.
     * - 4 bytes: the length of the big string in bytes.
     * - Following bytes: the big string data.
     * <p>
     * If not all big strings exist, the message is adjusted to reflect the actual number of big strings sent.
     *
     * @param toBuf The buffer to which the encoded message content will be written.
     */
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

    /**
     * Returns the length of the encoded message content in bytes.
     * <p>
     * The length is calculated as follows:
     * - HEADER_LENGTH for the header.
     * - For each big string to send:
     * - 8 bytes for the UUID.
     * - 4 bytes for the length of the big string.
     * - The length of the big string data.
     *
     * @return The length of the encoded message content in bytes.
     */
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