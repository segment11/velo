package io.velo.repl.incremental;

import io.velo.persist.LocalPersist;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class XReverseIndexPutWord implements BinlogContent {
    private final String lowerCaseWord;

    private final long longId;

    public XReverseIndexPutWord(String lowerCaseWord, long longId) {
        this.lowerCaseWord = lowerCaseWord;
        this.longId = longId;
    }

    private static final Logger log = LoggerFactory.getLogger(XReverseIndexPutWord.class);

    @Override
    public Type type() {
        return Type.reverse_index_put_word;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 8 bytes for long id, 2 bytes for word length, word bytes
        return 1 + 4 + 8 + 2 + lowerCaseWord.length();
    }

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

    @Override
    public void apply(short slot, ReplPair replPair) {
        var oneSlot = localPersist.oneSlot(slot);

        oneSlot.submitIndexJobRun(lowerCaseWord, (indexHandler) -> {
            indexHandler.putWordAndAddLongId(lowerCaseWord, longId);
        }).whenComplete((v, e) -> {
            if (e != null) {
                log.error("Submit index job put word and add long id error=" + e.getMessage());
                return;
            }

            oneSlot.submitIndexJobDone();
        });
    }
}
