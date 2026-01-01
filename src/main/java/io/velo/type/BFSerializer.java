package io.velo.type;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import static com.google.common.hash.BloomFilter.readFrom;

/**
 * A utility class for serializing and deserializing Bloom Filters.
 * This class provides methods to convert a Bloom Filter to a byte array and vice versa.
 */
public class BFSerializer {
    /**
     * Deserializes a Bloom Filter from a byte array.
     *
     * @param bytes  the byte array containing the serialized Bloom Filter
     * @param offset the starting offset in the byte array
     * @param length the number of bytes to read from the byte array
     * @return the deserialized Bloom Filter
     * @throws RuntimeException if an IOException occurs during deserialization
     */
    public static BloomFilter<CharSequence> fromBytes(byte[] bytes, int offset, int length) {
        BloomFilter<CharSequence> filter;
        try {
            filter = readFrom(
                    new ByteArrayInputStream(bytes, offset, length),
                    Funnels.stringFunnel(Charset.defaultCharset())
            );
            return filter;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Serializes a Bloom Filter to a byte array.
     *
     * @param filter the Bloom Filter to serialize
     * @return the byte array containing the serialized Bloom Filter
     * @throws RuntimeException if an IOException occurs during serialization
     */
    public static byte[] toBytes(BloomFilter<CharSequence> filter) {
        var bos = new java.io.ByteArrayOutputStream();
        try {
            filter.writeTo(bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}