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
import java.util.concurrent.atomic.LongAdder;

/**
 * Represents a dictionary used for compression and decompression with Zstd.
 * This class manages dictionary bytes, compression contexts, and provides methods for encoding and decoding dictionaries.
 */
public class Dict implements Serializable {

    /**
     * Sequence number for the self Zstd dictionary.
     */
    public static final int SELF_ZSTD_DICT_SEQ = 1;

    /**
     * Sequence number for the global Zstd dictionary.
     */
    public static final int GLOBAL_ZSTD_DICT_SEQ = 10;

    /**
     * Singleton instance of the self Zstd dictionary.
     */
    public static final Dict SELF_ZSTD_DICT = new Dict();

    /**
     * Singleton instance of the global Zstd dictionary.
     */
    public static final Dict GLOBAL_ZSTD_DICT = new Dict();

    /**
     * Key for the global Zstd dictionary.
     * Note: This key should not be a prefix of any other dictionary key.
     */
    public static final String GLOBAL_ZSTD_DICT_KEY = "dict-x-global";

    /**
     * File name for the global dictionary file.
     */
    static final String GLOBAL_DICT_FILE_NAME = "dict-global-raw.dat";

    /**
     * Maximum length of the global dictionary bytes for latency considerations.
     */
    private static final int GLOBAL_DICT_BYTES_MAX_LENGTH = 1024 * 16;

    /**
     * Logger for logging information and warnings.
     */
    private static final Logger log = LoggerFactory.getLogger(Dict.class);

    static {
        SELF_ZSTD_DICT.seq = SELF_ZSTD_DICT_SEQ;
        GLOBAL_ZSTD_DICT.seq = GLOBAL_ZSTD_DICT_SEQ;
    }

    /**
     * Resets the global dictionary bytes.
     *
     * @param dictBytes the new dictionary bytes
     * @throws IllegalStateException if the dictionary bytes are too long or empty
     */
    public static void resetGlobalDictBytes(byte[] dictBytes) {
        if (dictBytes.length == 0 || dictBytes.length > GLOBAL_DICT_BYTES_MAX_LENGTH) {
            throw new IllegalStateException("Dict global dict bytes too long=" + dictBytes.length);
        }

        GLOBAL_ZSTD_DICT.dictBytes = dictBytes;
        log.warn("Dict global dict bytes overwritten, dict bytes length={}", dictBytes.length);
        GLOBAL_ZSTD_DICT.closeCtx();
        GLOBAL_ZSTD_DICT.initCtx();
    }

    /**
     * Initializes the global dictionary bytes from a file.
     *
     * @param targetFile the file containing the dictionary bytes
     */
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

    /**
     * Saves the global dictionary bytes to a file.
     *
     * @param targetFile the file to save the dictionary bytes to
     */
    public static void saveGlobalDictBytesToFile(File targetFile) {
        try {
            Files.write(targetFile.toPath(), GLOBAL_ZSTD_DICT.dictBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Sequence number of the dictionary.
     */
    private int seq;

    /**
     * Returns the sequence number of the dictionary.
     *
     * @return the sequence number
     */
    public int getSeq() {
        return seq;
    }

    /**
     * Sets the sequence number of the dictionary.
     *
     * @param seq the sequence number to set
     */
    public void setSeq(int seq) {
        this.seq = seq;
    }

    /**
     * Creation time of the dictionary.
     */
    private long createdTime;

    /**
     * Returns the creation time of the dictionary.
     *
     * @return the creation time
     */
    public long getCreatedTime() {
        return createdTime;
    }

    /**
     * Sets the creation time of the dictionary.
     *
     * @param createdTime the creation time to set
     */
    public void setCreatedTime(long createdTime) {
        this.createdTime = createdTime;
    }

    /**
     * Dictionary bytes used for compression and decompression.
     */
    private byte[] dictBytes;

    /**
     * Returns the dictionary bytes.
     *
     * @return the dictionary bytes
     */
    public byte[] getDictBytes() {
        return dictBytes;
    }

    /**
     * Checks if the dictionary has valid dictionary bytes.
     *
     * @return true if the dictionary has valid dictionary bytes, false otherwise
     */
    public boolean hasDictBytes() {
        return dictBytes != null && dictBytes.length > 1;
    }

    /**
     * Sets the dictionary bytes.
     *
     * @param dictBytes the dictionary bytes to set
     */
    public void setDictBytes(byte[] dictBytes) {
        this.dictBytes = dictBytes;
    }

    /**
     * Total count of compressed data.
     */
    final LongAdder compressedCountTotal = new LongAdder();

    /**
     * Total bytes of compressed data.
     */
    final LongAdder compressedBytesTotal = new LongAdder();

    /**
     * Total bytes of data before compression.
     */
    final LongAdder compressBytesTotal = new LongAdder();

    /**
     * Calculates the compression ratio.
     *
     * @return the compression ratio
     */
    double compressedRatio() {
        var sumCompress = compressBytesTotal.sum();
        if (sumCompress == 0) {
            return 0;
        }

        var sumCompressed = compressedBytesTotal.sum();
        return (double) sumCompressed / sumCompress;
    }

    /**
     * Decompression contexts for each thread.
     */
    @VisibleForTesting
    @ThreadNeedLocal
    ZstdDecompressCtx[] decompressCtxArray;

    /**
     * Compression contexts for each thread.
     */
    @VisibleForTesting
    @ThreadNeedLocal
    ZstdCompressCtx[] ctxCompressArray;

    /**
     * Initializes the compression and decompression contexts.
     */
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

    /**
     * Closes the compression and decompression contexts.
     */
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

    /**
     * Compresses a byte array.
     *
     * @param src the source byte array to compress
     * @return the compressed byte array
     */
    public byte[] compressByteArray(byte[] src) {
        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread();
        var r = ctxCompressArray[threadIndex].compress(src);
        compressedCountTotal.add(1);
        compressBytesTotal.add(src.length);
        compressedBytesTotal.add(r.length);
        return r;
    }

    /**
     * Compresses a byte array into a destination byte array.
     *
     * @param dst       the destination byte array
     * @param dstOffset the offset in the destination byte array
     * @param src       the source byte array to compress
     * @param srcOffset the offset in the source byte array
     * @param length    the number of bytes to compress
     * @return the number of bytes written to the destination array
     */
    public int compressByteArray(byte[] dst, int dstOffset, byte[] src, int srcOffset, int length) {
        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread();
        var n = ctxCompressArray[threadIndex].compressByteArray(dst, dstOffset, dst.length - dstOffset, src, srcOffset, length);
        compressedCountTotal.add(1);
        compressBytesTotal.add(length);
        compressedBytesTotal.add(n);
        return n;
    }

    /**
     * Decompresses a byte array into a destination byte array.
     *
     * @param dst       the destination byte array
     * @param dstOffset the offset in the destination byte array
     * @param src       the source byte array to decompress
     * @param srcOffset the offset in the source byte array
     * @param length    the number of bytes to decompress
     * @return the number of bytes written to the destination array
     */
    public int decompressByteArray(byte[] dst, int dstOffset, byte[] src, int srcOffset, int length) {
        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread();
        return decompressCtxArray[threadIndex].decompressByteArray(dst, dstOffset, dst.length - dstOffset, src, srcOffset, length);
    }

    /**
     * Compresses a direct byte buffer.
     *
     * @param dstBuffer the destination byte buffer
     * @param dstOffset the offset in the destination byte buffer
     * @param dstSize   the size of the destination byte buffer
     * @param srcBuffer the source byte buffer to compress
     * @param srcOffset the offset in the source byte buffer
     * @param length    the number of bytes to compress
     * @return the number of bytes written to the destination buffer
     */
    public int compressByteBuffer(ByteBuffer dstBuffer, int dstOffset, int dstSize, ByteBuffer srcBuffer, int srcOffset, int length) {
        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread();
        var n = ctxCompressArray[threadIndex].compressDirectByteBuffer(dstBuffer, dstOffset, dstSize, srcBuffer, srcOffset, length);
        compressedCountTotal.add(1);
        compressBytesTotal.add(length);
        compressedBytesTotal.add(n);
        return n;
    }

    /**
     * Decompresses a direct byte buffer.
     *
     * @param dstBuffer the destination byte buffer
     * @param dstOffset the offset in the destination byte buffer
     * @param dstSize   the size of the destination byte buffer
     * @param srcBuffer the source byte buffer to decompress
     * @param srcOffset the offset in the source byte buffer
     * @param length    the number of bytes to decompress
     * @return the number of bytes written to the destination buffer
     */
    public int decompressByteBuffer(ByteBuffer dstBuffer, int dstOffset, int dstSize, ByteBuffer srcBuffer, int srcOffset, int length) {
        var threadIndex = MultiWorkerServer.STATIC_GLOBAL_V.getThreadLocalIndexByCurrentThread();
        return decompressCtxArray[threadIndex].decompressDirectByteBuffer(dstBuffer, dstOffset, dstSize, srcBuffer, srcOffset, length);
    }

    /**
     * Returns a string representation of the dictionary.
     *
     * @return a string representation of the dictionary
     */
    @Override
    public String toString() {
        return "Dict{" +
                "seq=" + seq +
                ", createdTime=" + new Date(createdTime) +
                ", dictBytes.length=" + (dictBytes == null ? 0 : dictBytes.length) +
                '}';
    }

    /**
     * Returns the hash code of the dictionary.
     *
     * @return the hash code
     */
    @Override
    public int hashCode() {
        return Objects.hashCode(seq);
    }

    /**
     * Checks if this dictionary is equal to another object.
     *
     * @param obj the object to compare to
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Dict dict = (Dict) obj;
        return seq == dict.seq;
    }

    /**
     * Length of the encoded dictionary header.
     * dict seq int + createdTime long + keyPrefix length short + dict length short
     */
    private static final int ENCODED_HEADER_LENGTH = 4 + 8 + 2 + 2;

    /**
     * Calculates the length of the encoded dictionary.
     *
     * @param keyPrefix the key prefix to include in the encoding
     * @return the length of the encoded dictionary
     */
    public int encodeLength(String keyPrefix) {
        return 4 + ENCODED_HEADER_LENGTH + keyPrefix.length() + dictBytes.length;
    }

    /**
     * Encodes the dictionary with a key prefix or suffix.
     *
     * @param keyPrefixOrSuffix the key prefix to include in the encoding
     * @return the encoded dictionary bytes
     */
    public byte[] encode(String keyPrefixOrSuffix) {
        int vLength = ENCODED_HEADER_LENGTH + keyPrefixOrSuffix.length() + dictBytes.length;

        var bytes = new byte[4 + vLength];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.putInt(vLength);
        buffer.putInt(seq);
        buffer.putLong(createdTime);
        buffer.putShort((short) keyPrefixOrSuffix.length());
        buffer.put(keyPrefixOrSuffix.getBytes());
        buffer.putShort((short) dictBytes.length);
        buffer.put(dictBytes);

        return bytes;
    }

    /**
     * Record representing a dictionary with a key prefix or suffix.
     */
    public record DictWithKeyPrefixOrSuffix(String keyPrefixOrSuffix, Dict dict) {
        /**
         * Returns a string representation of the dictionary with key prefix or suffix.
         *
         * @return a string representation of the dictionary with key prefix or suffix
         */
        @Override
        public String toString() {
            return "DictWithKeyPrefixOrSuffix{" +
                    "keyPrefixOrSuffix='" + keyPrefixOrSuffix + '\'' +
                    ", dict=" + dict +
                    '}';
        }
    }

    /**
     * Decodes a dictionary from a data input stream.
     *
     * @param is the data input stream to read from
     * @return the decoded dictionary with key prefix or suffix
     * @throws IOException if an I/O error occurs
     */
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

    /**
     * Constructs a new dictionary with default values.
     */
    public Dict() {
        // one dict byte means not set yet
        this.dictBytes = new byte[1];
        this.seq = SELF_ZSTD_DICT_SEQ;
        this.createdTime = System.currentTimeMillis();
    }

    /**
     * Generates a random sequence number for a new dictionary.
     *
     * @return a random sequence number
     */
    static int generateRandomSeq() {
        var random = new Random();
        return random.nextInt(1000) * 1000 * 1000 +
                random.nextInt(1000) * 1000 +
                random.nextInt(1000) +
                GLOBAL_ZSTD_DICT_SEQ;
    }

    /**
     * Constructs a new dictionary with the specified dictionary bytes.
     *
     * @param dictBytes the dictionary bytes
     * @throws IllegalArgumentException if the dictionary bytes are too long
     */
    public Dict(byte[] dictBytes) {
        if (dictBytes.length > Short.MAX_VALUE) {
            throw new IllegalArgumentException("Dict bytes too long=" + dictBytes.length);
        }

        this.dictBytes = dictBytes;
        this.seq = generateRandomSeq();
        this.createdTime = System.currentTimeMillis();
    }
}
