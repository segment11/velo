package io.velo.type;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * Bloom Filter implementation with dynamic expansion capability.
 */
public class RedisBF {
    /**
     * Single Bloom Filter with capacity and item count.
     */
    static class One {
        /** The Bloom Filter instance. */
        BloomFilter<CharSequence> filter;

        /** Number of items inserted. */
        int itemInserted;

        /** Capacity of the Bloom Filter. */
        int capacity;

        /**
         * @return true if more than 90% of capacity is used
         */
        public boolean isFull() {
            if (capacity == 0) {
                return true;
            }
            return itemInserted / (double) capacity >= 0.9;
        }
    }

    /** Default capacity for each Bloom Filter. */
    public static final int DEFAULT_CAPACITY = 100;

    /** Default false positive probability. */
    public static final double DEFAULT_FPP = 0.01;

    /** Default expansion factor for scaling. */
    public static final byte DEFAULT_EXPANSION = 2;

    /** Maximum expansion factor. */
    public static final byte MAX_EXPANSION = 10;

    private static final Funnel<CharSequence> STRING_FUNNEL = Funnels.stringFunnel(StandardCharsets.UTF_8);

    private double fpp;
    private byte expansion;
    private boolean nonScaling;

    /**
     * @return the expansion factor
     */
    public byte getExpansion() {
        return expansion;
    }

    /**
     * @return true if non-scaling Bloom Filter
     */
    public boolean isNonScaling() {
        return nonScaling;
    }

    private static final byte MAX_LIST_SIZE = 4;

    private final ArrayList<One> list = new ArrayList<>();

    private RedisBF() {
    }

    /**
     * @param initByDefault whether to initialize with defaults
     */
    public RedisBF(boolean initByDefault) {
        this(DEFAULT_CAPACITY, DEFAULT_FPP, DEFAULT_EXPANSION, false);
    }

    /**
     * @param initCapacity  initial capacity for each Bloom Filter
     * @param initFpp       false positive probability
     * @param initExpansion expansion factor for scaling
     * @param nonScaling    whether the Bloom Filter is non-scaling
     */
    public RedisBF(int initCapacity, double initFpp, byte initExpansion, boolean nonScaling) {
        if (initExpansion <= 0) {
            throw new IllegalArgumentException("BF expansion must be positive, got: " + initExpansion);
        }
        this.fpp = initFpp;
        this.expansion = initExpansion;
        this.nonScaling = nonScaling;

        var one = new One();
        one.filter = BloomFilter.create(STRING_FUNNEL, initCapacity, initFpp);
        one.capacity = initCapacity;
        list.add(one);
    }

    /**
     * @return total items inserted across all Bloom Filters
     */
    public int itemInserted() {
        return list.stream().mapToInt(one -> one.itemInserted).sum();
    }

    /**
     * @return total capacity across all Bloom Filters
     */
    public int capacity() {
        return list.stream().mapToInt(one -> one.capacity).sum();
    }

    /**
     * @return number of Bloom Filters in the list
     */
    public int listSize() {
        return list.size();
    }

    /**
     * @return estimated memory allocation in bytes
     */
    public int memoryAllocatedEstimate() {
        long totalBits = 0;
        for (One one : list) {
            double m = -one.capacity * Math.log(fpp) / (Math.log(2) * Math.log(2));
            totalBits += (long) Math.ceil(m);
        }
        return (int) ((totalBits + 7) / 8);
    }

    /**
     * @param item the item to check
     * @return true if the item might be in any Bloom Filter
     */
    public boolean mightContain(String item) {
        return list.stream().anyMatch(one -> one.filter.mightContain(item));
    }

    /**
     * @param item the item to add
     * @return true if added, false if already present
     */
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

        if (nonScaling) {
            throw new RuntimeException("BF sub filter can not be expanded");
        }

        if (list.size() >= MAX_LIST_SIZE) {
            throw new RuntimeException("BF sub filter list size is too large");
        }

        var lastOne = list.getLast();
        if (lastOne.capacity > Integer.MAX_VALUE / expansion) {
            throw new RuntimeException("BF capacity overflow");
        }
        var newOneCapacity = lastOne.capacity * expansion;

        var newOne = new One();
        newOne.filter = BloomFilter.create(STRING_FUNNEL, newOneCapacity, fpp);
        newOne.capacity = newOneCapacity;
        newOne.filter.put(item);
        newOne.itemInserted = 1;
        list.add(newOne);

        return true;
    }

    /**
     * @return encoded byte array
     */
    public byte[] encode() {
        ArrayList<byte[]> filterBytesList = new ArrayList<>();
        for (var one : list) {
            var bytes = BFSerializer.toBytes(one.filter);
            filterBytesList.add(bytes);
        }

        var length = 1 + 1 + 8 + 4;
        for (int i = 0; i < list.size(); i++) {
            var filterBytes = filterBytesList.get(i);
            length += 4 + filterBytes.length;
            length += 4 + 4;
        }

        var bytes = new byte[length];
        var buffer = ByteBuffer.wrap(bytes);

        buffer.put(expansion);
        buffer.put((byte) (nonScaling ? 1 : 0));
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

    /**
     * @param data the byte array to decode
     * @return the RedisBF object
     */
    public static RedisBF decode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);

        var r = new RedisBF();
        r.expansion = buffer.get();
        if (r.expansion <= 0) {
            throw new IllegalStateException("BF expansion must be positive, got: " + r.expansion);
        }
        r.nonScaling = buffer.get() != 0;
        r.fpp = buffer.getDouble();
        var listSize = buffer.getInt();
        if (listSize <= 0 || listSize > MAX_LIST_SIZE) {
            throw new IllegalStateException(
                    "BF list size must be between 1 and " + MAX_LIST_SIZE + ", got: " + listSize);
        }
        for (int i = 0; i < listSize; i++) {
            var filterBytesLength = buffer.getInt();
            if (filterBytesLength <= 0) {
                throw new IllegalStateException(
                        "Invalid filter bytes length: " + filterBytesLength + ", expected > 0");
            }
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
