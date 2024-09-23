package io.velo;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.util.Date;
import java.util.Objects;
import java.util.Random;

public class Dict implements Serializable {
    public static final int SELF_ZSTD_DICT_SEQ = 1;
    public static final int GLOBAL_ZSTD_DICT_SEQ = 10;

    public static final Dict SELF_ZSTD_DICT = new Dict();
    public static final Dict GLOBAL_ZSTD_DICT = new Dict();
    // warning: this key can not be other dict key prefix
    public static final String GLOBAL_ZSTD_DICT_KEY = "dict-x-global";

    static final String GLOBAL_DICT_FILE_NAME = "dict-global-raw.dat";
    // for latency
    private static final int GLOBAL_DICT_BYTES_MAX_LENGTH = 1024 * 16;

    private static final Logger log = LoggerFactory.getLogger(Dict.class);

    static {
        SELF_ZSTD_DICT.seq = SELF_ZSTD_DICT_SEQ;
        GLOBAL_ZSTD_DICT.seq = GLOBAL_ZSTD_DICT_SEQ;
    }

    public static void resetGlobalDictBytes(byte[] dictBytes) {
        if (dictBytes.length == 0 || dictBytes.length > GLOBAL_DICT_BYTES_MAX_LENGTH) {
            throw new IllegalStateException("Dict global dict bytes too long=" + dictBytes.length);
        }

        GLOBAL_ZSTD_DICT.dictBytes = dictBytes;
        log.warn("Dict global dict bytes overwritten, dict bytes length={}", dictBytes.length);
        GLOBAL_ZSTD_DICT.closeCtx();
        GLOBAL_ZSTD_DICT.initCtx();
    }

    public static void initGlobalDictBytesByFile(File targetFile) {
        if (!targetFile.exists()) {
            log.warn("Dict global dict file not exists={}", targetFile.getAbsolutePath());
            return;
        }

        byte[] dictBytes;
        try {
            dictBytes = Files.readAllBytes(targetFile.toPath());
            resetGlobalDictBytes(dictBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void saveGlobalDictBytesToFile(File targetFile) {
        try {
            Files.write(targetFile.toPath(), GLOBAL_ZSTD_DICT.dictBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private int seq;

    public int getSeq() {
        return seq;
    }

    public void setSeq(int seq) {
        this.seq = seq;
    }

    private long createdTime;

    public long getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    private byte[] dictBytes;

    public byte[] getDictBytes() {
        return dictBytes;
    }

    public boolean hasDictBytes() {
        // global / self dict, dict bytes length is 1
        return dictBytes != null && dictBytes.length > 1;
    }

    public void setDictBytes(byte[] dictBytes) {
        this.dictBytes = dictBytes;
    }

    @VisibleForTesting
    ZstdDecompressCtx[] decompressCtxArray;
    @VisibleForTesting
    ZstdCompressCtx[] ctxCompressArray;

    void initCtx() {
        if (decompressCtxArray == null) {
            decompressCtxArray = new ZstdDecompressCtx[ConfForGlobal.netWorkers];
            for (int i = 0; i < decompressCtxArray.length; i++) {
                decompressCtxArray[i] = new ZstdDecompressCtx();
                decompressCtxArray[i].loadDict(dictBytes);
            }
            log.info("Dict init decompress ctx, dict bytes length={}, net workers={}", dictBytes.length, decompressCtxArray.length);
        }

        if (ctxCompressArray == null) {
            ctxCompressArray = new ZstdCompressCtx[ConfForGlobal.netWorkers];
            for (int i = 0; i < ctxCompressArray.length; i++) {
                ctxCompressArray[i] = new ZstdCompressCtx();
                ctxCompressArray[i].loadDict(dictBytes);
            }
            log.info("Dict init compress ctx, dict bytes length={}, net workers={}", dictBytes.length, ctxCompressArray.length);
        }
    }

    void closeCtx() {
        if (decompressCtxArray != null) {
            for (var zstdDecompressCtx : decompressCtxArray) {
                zstdDecompressCtx.close();
            }
            log.warn("Dict close decompress ctx, dict bytes length={}, net workers={}", dictBytes.length, decompressCtxArray.length);
            decompressCtxArray = null;
        }

        if (ctxCompressArray != null) {
            for (var zstdCompressCtx : ctxCompressArray) {
                zstdCompressCtx.close();
            }
            log.warn("Dict close compress ctx, dict bytes length={}, net workers={}", dictBytes.length, ctxCompressArray.length);
            ctxCompressArray = null;
        }
    }

    public byte[] compressByteArray(byte[] src) {
        return ctxCompressArray[MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread()].compress(src);
    }

    public int compressByteArray(byte[] dst, int dstOffset, byte[] src, int srcOffset, int length) {
        return ctxCompressArray[MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread()].compressByteArray(dst, dstOffset, dst.length - dstOffset, src, srcOffset, length);
    }

    public int decompressByteArray(byte[] dst, int dstOffset, byte[] src, int srcOffset, int length) {
        return decompressCtxArray[MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread()].decompressByteArray(dst, dstOffset, dst.length - dstOffset, src, srcOffset, length);
    }

    @Override
    public String toString() {
        return "Dict{" +
                "seq=" + seq +
                ", createdTime=" + new Date(createdTime) +
                ", dictBytes.length=" + (dictBytes == null ? 0 : dictBytes.length) +
                '}';
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(seq);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Dict dict = (Dict) obj;
        return seq == dict.seq;
    }

    // seq int + create time long + key prefix length short + key prefix + dict bytes length short + dict bytes
    private static final int ENCODED_HEADER_LENGTH = 4 + 8 + 2 + 2;

    public int encodeLength(String keyPrefix) {
        return 4 + ENCODED_HEADER_LENGTH + keyPrefix.length() + dictBytes.length;
    }

    public byte[] encode(String keyPrefix) {
        int vLength = ENCODED_HEADER_LENGTH + keyPrefix.length() + dictBytes.length;

        var bytes = new byte[4 + vLength];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.putInt(vLength);
        buffer.putInt(seq);
        buffer.putLong(createdTime);
        buffer.putShort((short) keyPrefix.length());
        buffer.put(keyPrefix.getBytes());
        buffer.putShort((short) dictBytes.length);
        buffer.put(dictBytes);

        return bytes;
    }

    public record DictWithKeyPrefixOrSuffix(String keyPrefixOrSuffix, Dict dict) {
        @Override
        public String toString() {
            return "DictWithKeyPrefixOrSuffix{" +
                    "keyPrefixOrSuffix='" + keyPrefixOrSuffix + '\'' +
                    ", dict=" + dict +
                    '}';
        }
    }

    public static DictWithKeyPrefixOrSuffix decode(DataInputStream is) throws IOException {
        if (is.available() < 4) {
            return null;
        }

        var vLength = is.readInt();
        if (vLength == 0) {
            return null;
        }

        var seq = is.readInt();
        var createdTime = is.readLong();
        var keyPrefixOrSuffixLength = is.readShort();
        if (keyPrefixOrSuffixLength > CompressedValue.KEY_MAX_LENGTH || keyPrefixOrSuffixLength <= 0) {
            throw new IllegalStateException("Key prefix or suffix length error, key prefix or suffix length=" + keyPrefixOrSuffixLength);
        }

        var keyPrefixOrSuffixBytes = new byte[keyPrefixOrSuffixLength];
        is.readFully(keyPrefixOrSuffixBytes);
        var dictBytesLength = is.readShort();
        var dictBytes = new byte[dictBytesLength];
        is.readFully(dictBytes);

        if (vLength != ENCODED_HEADER_LENGTH + keyPrefixOrSuffixLength + dictBytesLength) {
            throw new IllegalStateException("Invalid length=" + vLength);
        }

        var dict = new Dict();
        dict.seq = seq;
        dict.createdTime = createdTime;
        dict.dictBytes = dictBytes;

        return new DictWithKeyPrefixOrSuffix(new String(keyPrefixOrSuffixBytes), dict);
    }

    public Dict() {
        this.dictBytes = new byte[1];
        this.seq = SELF_ZSTD_DICT_SEQ;
        this.createdTime = System.currentTimeMillis();
    }

    // still may be conflict, when slave change to master, new master create new dict with same seq
    static int generateRandomSeq() {
        var random = new Random();
        return random.nextInt(1000) * 1000 * 1000 +
                random.nextInt(1000) * 1000 +
                random.nextInt(1000) +
                GLOBAL_ZSTD_DICT_SEQ;
    }

    // only create when train new dict by TrainSampleJob
    public Dict(byte[] dictBytes) {
        if (dictBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Dict bytes too long=" + dictBytes.length);
        }

        this.dictBytes = dictBytes;
        this.seq = generateRandomSeq();
        this.createdTime = System.currentTimeMillis();
    }
}
