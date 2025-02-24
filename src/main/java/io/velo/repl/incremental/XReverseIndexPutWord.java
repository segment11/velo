package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Represents the binary log content for a reverse index put word operation in the Velo replication system.
 * This class encapsulates the necessary information required for both the master to create the binlog entry
 * and the slave to apply the corresponding operation during replication.
 */
public class XReverseIndexPutWord implements BinlogContent {
    private final String lowerCaseWord;
    private final long longId;

    /**
     * Retrieves the lower case word associated with this reverse index operation.
     *
     * @return The lower case word.
     */
    public String getLowerCaseWord() {
        return lowerCaseWord;
    }

    /**
     * Retrieves the long ID associated with this reverse index operation.
     *
     * @return The long ID.
     */
    public long getLongId() {
        return longId;
    }

    /**
     * Constructs a new XReverseIndexPutWord object with the specified lower case word and long ID.
     *
     * @param lowerCaseWord The lower case word associated with the operation.
     * @param longId        The long ID associated with the operation.
     */
    public XReverseIndexPutWord(String lowerCaseWord, long longId) {
        this.lowerCaseWord = lowerCaseWord;
        this.longId = longId;
    }

    private static final Logger log = LoggerFactory.getLogger(XReverseIndexPutWord.class);

    /**
     * Returns the type of this binlog content.
     *
     * @return The type of this binlog content.
     */
    @Override
    public Type type() {
        return Type.reverse_index_put_word;
    }

    /**
     * Calculates the total number of bytes required to encode this binlog content.
     *
     * @return The total number of bytes required for encoding.
     */
    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 8 bytes for long id, 2 bytes for word length, word bytes
        return 1 + 4 + 8 + 2 + lowerCaseWord.length();
    }

    /**
     * Encodes this binlog content into a byte array, including the type byte and length check.
     *
     * @return The byte array representation of this binlog content.
     */
    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);
        buffer.putLong(longId);
        buffer.putShort((short) lowerCaseWord.length());
        buffer.put(lowerCaseWord.getBytes());

        return bytes;
    }

    /**
     * Decodes a binlog content from the provided ByteBuffer.
     *
     * @param buffer The ByteBuffer containing the encoded binlog content.
     * @return The decoded XReverseIndexPutWord object.
     */
    public static XReverseIndexPutWord decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var longId = buffer.getLong();
        var wordLength = buffer.getShort();
        var wordBytes = new byte[wordLength];
        buffer.get(wordBytes);
        var lowerCaseWord = new String(wordBytes);

        return new XReverseIndexPutWord(lowerCaseWord, longId);
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * Applies this binlog content to the specified replication slot and repl pair.
     * This method submits a job to update the reverse index by putting the word and adding the long ID.
     *
     * @param slot     The replication slot to which this content is applied.
     * @param replPair The repl pair associated with this replication session.
     */
    @Override
    public void apply(short slot, ReplPair replPair) {
        var oneSlot = localPersist.oneSlot(slot);

        oneSlot.submitIndexJobRun(lowerCaseWord, (indexHandler) -> {
            indexHandler.putWordAndAddLongId(lowerCaseWord, longId);
        }).whenComplete((v, e) -> {
            if (e != null) {
                log.error("Submit index job put word and add long id error={}", e.getMessage());
                return;
            }

            oneSlot.submitIndexJobDone();
        });
    }
}