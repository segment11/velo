package io.velo.type;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import static com.google.common.hash.BloomFilter.readFrom;

public class BFSerializer {
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
