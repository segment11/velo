package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.persist.BigStringFiles;
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
    private final int bucketIndex;
    private final File bigStringDir;
    private final List<BigStringFiles.IdWithKey> toSendIdList;
    private final boolean isSendAllOnce;

    /**
     * The maximum number of big strings that can be sent in one batch.
     */
    static final int ONCE_SEND_BIG_STRING_COUNT = 10;

    /**
     * Constructs a new {@link ToSlaveExistsBigString} message.
     *
     * @param bucketIndex    the bucket index in which the big strings are stored
     * @param bigStringDir   the directory containing the big strings (files)
     * @param idListInMaster the list of ids of big strings in the master
     * @param sentIdList     the list of ids of big strings that have already been sent to the slave
     */
    public ToSlaveExistsBigString(int bucketIndex, File bigStringDir, List<BigStringFiles.IdWithKey> idListInMaster, List<BigStringFiles.IdWithKey> sentIdList) {
        this.bucketIndex = bucketIndex;
        this.bigStringDir = bigStringDir;

        var toSendIdList = new ArrayList<BigStringFiles.IdWithKey>();
        // Exclude UUIDs that have already been sent
        for (var id : idListInMaster) {
            var isSentExist = false;
            for (var id2 : sentIdList) {
                if (id2.uuid() == id.uuid() && id2.keyHash() == id.keyHash()) {
                    isSentExist = true;
                    break;
                }
            }
            if (!isSentExist) {
                toSendIdList.add(id);
            }
        }

        this.isSendAllOnce = toSendIdList.size() <= ONCE_SEND_BIG_STRING_COUNT;
        if (!isSendAllOnce) {
            toSendIdList = new ArrayList<>(toSendIdList.subList(0, ONCE_SEND_BIG_STRING_COUNT));
        }

        this.toSendIdList = toSendIdList;
    }

    /**
     * The length of the header in the encoded message, which includes:
     * - 4 bytes for the bucket index.
     * - 4 bytes for the(send big string count).
     * - 1 byte as a flag indicating whether all big strings are sent in one batch.
     */
    private static final int HEADER_LENGTH = 4 + 4 + 1;

    /**
     * Encodes the content of this message to the given {@link ByteBuf}.
     * <p>
     * The encoding format is as follows:
     * - First 4 bytes: the bucket index.
     * - Next 4 bytes: the count of big strings to send.
     * - Next 1 byte: a flag indicating if all big strings are sent in one batch (1 for true, 0 for false).
     * - For each big string to send:
     * - 8 bytes: the UUID of the big string.
     * - 8 bytes: the key hash of the big string.
     * - 4 bytes: the length of the big string in bytes.
     * - Following bytes: the big string data.
     * <p>
     * If not all big strings exist, the message is adjusted to reflect the actual number of big strings sent.
     *
     * @param toBuf the buffer to which the encoded message content will be written
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeInt(bucketIndex);

        if (toSendIdList.isEmpty()) {
            toBuf.writeInt(0);
            toBuf.writeByte((byte) 1);
            return;
        }

        toBuf.writeInt(toSendIdList.size());
        toBuf.writeByte((byte) (isSendAllOnce ? 1 : 0));

        var existCount = 0;
        for (var id : toSendIdList) {
            var file = new File(bigStringDir, bucketIndex + "/" + id.uuid() + "_" + id.keyHash());
            if (!file.exists()) {
                continue;
            }

            existCount++;

            toBuf.writeLong(id.uuid());
            toBuf.writeLong(id.keyHash());
            var bigStringBytesLength = (int) file.length();
            toBuf.writeInt(bigStringBytesLength);

            try {
                var bytes = FileUtils.readFileToByteArray(file);
                toBuf.write(bytes);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        if (existCount != toSendIdList.size()) {
            toBuf.tail(4);
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
     * - 8 bytes for the key hash
     * - 4 bytes for the length of the big string.
     * - The length of the big string data.
     *
     * @return the length of the encoded message content in bytes
     */
    @Override
    public int encodeLength() {
        if (toSendIdList.isEmpty()) {
            return HEADER_LENGTH;
        }

        var length = HEADER_LENGTH;
        for (var id : toSendIdList) {
            var file = new File(bigStringDir, bucketIndex + "/" + id.uuid() + "_" + id.keyHash());
            if (!file.exists()) {
                continue;
            }

            var bigStringBytesLength = (int) file.length();
            length += 8 + 8 + 4 + bigStringBytesLength;
        }
        return length;
    }
}