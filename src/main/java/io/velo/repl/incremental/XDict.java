package io.velo.repl.incremental;

import io.velo.CompressedValue;
import io.velo.Dict;
import io.velo.DictMap;
import io.velo.repl.BinlogContent;
import io.velo.repl.ReplPair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class XDict implements BinlogContent {
    private final String keyPrefixOrSuffix;

    private final Dict dict;

    public String getKeyPrefixOrSuffix() {
        return keyPrefixOrSuffix;
    }

    public Dict getDict() {
        return dict;
    }

    public XDict(String keyPrefixOrSuffix, Dict dict) {
        this.keyPrefixOrSuffix = keyPrefixOrSuffix;
        this.dict = dict;
    }

    private static final Logger log = LoggerFactory.getLogger(XDict.class);

    @Override
    public Type type() {
        return BinlogContent.Type.dict;
    }

    @Override
    public int encodedLength() {
        // 1 byte for type, 4 bytes for encoded length for check
        // 4 bytes for seq, 8 bytes for created time
        // 2 bytes for key prefix length, key prefix, 2 bytes for dict bytes length, dict bytes
        return 1 + 4 + 4 + 8 + 2 + keyPrefixOrSuffix.length() + 2 + dict.getDictBytes().length;
    }

    @Override
    public byte[] encodeWithType() {
        var bytes = new byte[encodedLength()];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(type().code());
        buffer.putInt(bytes.length);
        buffer.putInt(dict.getSeq());
        buffer.putLong(dict.getCreatedTime());
        buffer.putShort((short) keyPrefixOrSuffix.length());
        buffer.put(keyPrefixOrSuffix.getBytes());
        buffer.putShort((short) dict.getDictBytes().length);
        buffer.put(dict.getDictBytes());

        return bytes;
    }

    public static XDict decodeFrom(ByteBuffer buffer) {
        // already read type byte
        var encodedLength = buffer.getInt();

        var seq = buffer.getInt();
        var createdTime = buffer.getLong();
        var keyPrefixLength = buffer.getShort();


        if (keyPrefixLength > CompressedValue.KEY_MAX_LENGTH || keyPrefixLength <= 0) {
            throw new IllegalStateException("Key prefix length error, key prefix length=" + keyPrefixLength);
        }

        var keyPrefixBytes = new byte[keyPrefixLength];
        buffer.get(keyPrefixBytes);
        var keyPrefix = new String(keyPrefixBytes);
        var dictBytesLength = buffer.getShort();
        var dictBytes = new byte[dictBytesLength];
        buffer.get(dictBytes);

        var dict = new Dict();
        dict.setSeq(seq);
        dict.setCreatedTime(createdTime);
        dict.setDictBytes(dictBytes);

        var r = new XDict(keyPrefix, dict);
        if (encodedLength != r.encodedLength()) {
            throw new IllegalStateException("Invalid encoded length=" + encodedLength);
        }
        return r;
    }

    @Override
    public void apply(short slot, ReplPair replPair) {
        log.warn("Repl slave get dict, key prefix or suffix={}, seq={}", keyPrefixOrSuffix, dict.getSeq());
        // ignore slot, need sync
        var dictMap = DictMap.getInstance();
        synchronized (dictMap) {
            dictMap.putDict(keyPrefixOrSuffix, dict);
        }
    }
}
