package io.velo.type;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;

public class RedisBF {
    static class One {
        BloomFilter<CharSequence> filter;
        int itemInserted;
        int capacity;

        public boolean isFull() {
            return itemInserted / (double) capacity >= 0.9;
        }
    }

    private static final int DEFAULT_CAPACITY = 100;
    private static final double DEFAULT_FPP = 0.01;
    private static final byte DEFAULT_EXPANSION = 2;

    private static final Funnel<CharSequence> STRING_FUNNEL = Funnels.stringFunnel(Charset.defaultCharset());

    @VisibleForTesting
    double fpp;
    @VisibleForTesting
    byte expansion;
    @VisibleForTesting
    boolean isNoScaling;

    private static final byte MAX_LIST_SIZE = 4;
    private final ArrayList<One> list = new ArrayList<>();

    private RedisBF() {
    }

    public RedisBF(boolean initByDefault) {
        this(DEFAULT_CAPACITY, DEFAULT_FPP, DEFAULT_EXPANSION, false);
    }

    public RedisBF(int initCapacity, double initFpp, byte expansion, boolean isNoScaling) {
        this.fpp = initFpp;
        this.expansion = expansion;
        this.isNoScaling = isNoScaling;

        // add first one
        var one = new One();
        one.filter = BloomFilter.create(STRING_FUNNEL, initCapacity, initFpp);
        one.capacity = initCapacity;
        list.add(one);
    }

    public int itemInserted() {
        return list.stream().mapToInt(one -> one.itemInserted).sum();
    }

    public int capacity() {
        return list.stream().mapToInt(one -> one.capacity).sum();
    }

    public int listSize() {
        return list.size();
    }

    public boolean mightContain(String item) {
        return list.stream().anyMatch(one -> one.filter.mightContain(item));
    }

    public boolean put(String item) {
        for (var one : list) {
            if (one.filter.mightContain(item)) {
                return false;
            }
        }

        boolean isPut = false;
        for (var one : list) {
            if (one.isFull()) {
                continue;
            }

            if (one.filter.put(item)) {
                isPut = true;
                one.itemInserted++;
                break;
            }
        }

        if (isPut) {
            return true;
        }

        // need expansion
        if (isNoScaling) {
            throw new RuntimeException("BF sub filter can not be expanded");
        }

        if (list.size() >= MAX_LIST_SIZE) {
            throw new RuntimeException("BF sub filter list size is too large");
        }

        var lastOne = list.getLast();
        var newOneCapacity = lastOne.capacity * expansion;

        var newOne = new One();
        newOne.filter = BloomFilter.create(STRING_FUNNEL, newOneCapacity, fpp);
        newOne.capacity = newOneCapacity;
        newOne.filter.put(item);
        newOne.itemInserted = 1;
        list.add(newOne);

        return true;
    }

    public byte[] encode() {
        ArrayList<byte[]> filterBytesList = new ArrayList<>();
        for (var one : list) {
            var bytes = BFSerializer.toBytes(one.filter);
            filterBytesList.add(bytes);
        }

        // 1 byte expansion, 1 byte isNoScaling, 8 byte fpp, 4 bytes for list size
        var length = 1 + 1 + 8 + 4;
        for (int i = 0; i < list.size(); i++) {
            var filterBytes = filterBytesList.get(i);
            // 4 bytes for bf bytes length, bf bytes
            length += 4 + filterBytes.length;
            // 4 bytes for item inserted, 4 bytes for capacity
            length += 4 + 4;
        }

        var bytes = new byte[length];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(expansion);
        buffer.put((byte) (isNoScaling ? 1 : 0));
        buffer.putDouble(fpp);
        buffer.putInt(list.size());
        for (int i = 0; i < list.size(); i++) {
            var one = list.get(i);
            var filterBytes = filterBytesList.get(i);
            buffer.putInt(filterBytes.length);
            buffer.put(filterBytes);
            buffer.putInt(one.itemInserted);
            buffer.putInt(one.capacity);
        }

        return bytes;
    }

    public static RedisBF decode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);

        var r = new RedisBF();
        r.expansion = buffer.get();
        r.isNoScaling = buffer.get() != 0;
        r.fpp = buffer.getDouble();
        var listSize = buffer.getInt();
        for (int i = 0; i < listSize; i++) {
            var filterBytesLength = buffer.getInt();
            var filterBytes = new byte[filterBytesLength];
            buffer.get(filterBytes);
            var filter = BFSerializer.fromBytes(filterBytes, 0, filterBytes.length);
            var itemInserted = buffer.getInt();
            var capacity = buffer.getInt();

            var one = new One();
            one.filter = filter;
            one.itemInserted = itemInserted;
            one.capacity = capacity;
            r.list.add(one);
        }

        return r;
    }

}
