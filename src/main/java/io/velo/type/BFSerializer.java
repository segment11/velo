package io.velo.type;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.google.common.hash.BloomFilter.readFrom;

/**
 * Utility for serializing and deserializing Bloom Filters.
 */
public class BFSerializer {
    /**
     * @param bytes  the serialized Bloom Filter bytes
     * @param offset the starting offset in the byte array
     * @param length the number of bytes to read
     * @return the deserialized Bloom Filter
     */
    public static BloomFilter<CharSequence> fromBytes(byte[] bytes, int offset, int length) {
        BloomFilter<CharSequence> filter;
        try {
            filter = readFrom(
                    new ByteArrayInputStream(bytes, offset, length),
                    Funnels.stringFunnel(StandardCharsets.UTF_8)
            );
            return filter;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * @param filter the Bloom Filter to serialize
     * @return the serialized bytes
     */
    public static byte[] toBytes(BloomFilter<CharSequence> filter) {
        var bos = new ByteArrayOutputStream();
        try {
            filter.writeTo(bos);
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
