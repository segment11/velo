package io.velo.type;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Funnels;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;

/**
 * A class representing a Bloom Filter that can dynamically expand to accommodate more items.
 * This class manages multiple Bloom Filters and provides methods for encoding and decoding the data.
 */
public class RedisBF {
    /**
     * A class representing a single Bloom Filter along with its capacity and item count.
     */
    static class One {
        /**
         * The Bloom Filter instance.
         */
        BloomFilter<CharSequence> filter;

        /**
         * The number of items inserted into the Bloom Filter.
         */
        int itemInserted;

        /**
         * The capacity of the Bloom Filter.
         */
        int capacity;

        /**
         * Checks if the Bloom Filter is full (i.e., more than 90% of its capacity is used).
         *
         * @return true if the Bloom Filter is full, false otherwise
         */
        public boolean isFull() {
            return itemInserted / (double) capacity >= 0.9;
        }
    }

    /**
     * The default capacity for each Bloom Filter.
     */
    public static final int DEFAULT_CAPACITY = 100;

    /**
     * The default false positive probability for each Bloom Filter.
     */
    public static final double DEFAULT_FPP = 0.01;

    /**
     * The default expansion factor for scaling the Bloom Filters.
     */
    public static final byte DEFAULT_EXPANSION = 2;

    /**
     * The maximum expansion factor for scaling the Bloom Filters.
     */
    public static final byte MAX_EXPANSION = 10;

    /**
     * The funnel used to convert strings into a format suitable for the Bloom Filter.
     */
    private static final Funnel<CharSequence> STRING_FUNNEL = Funnels.stringFunnel(Charset.defaultCharset());

    /**
     * The false positive probability for the Bloom Filters.
     */
    private double fpp;

    /**
     * The expansion factor for scaling the Bloom Filters.
     */
    private byte expansion;

    /**
     * A flag indicating whether the Bloom Filter is non-scaling.
     */
    private boolean nonScaling;

    /**
     * Returns the expansion factor for the Bloom Filters.
     *
     * @return the expansion factor
     */
    public byte getExpansion() {
        return expansion;
    }

    /**
     * Checks if the Bloom Filter is non-scaling.
     *
     * @return true if the Bloom Filter is non-scaling, false otherwise
     */
    public boolean isNonScaling() {
        return nonScaling;
    }

    /**
     * The maximum number of Bloom Filters in the list.
     */
    private static final byte MAX_LIST_SIZE = 4;

    /**
     * The list of Bloom Filters.
     */
    private final ArrayList<One> list = new ArrayList<>();

    /**
     * Private constructor to prevent instantiation without parameters.
     */
    private RedisBF() {
    }

    /**
     * Constructs a new instance of RedisBF with default settings.
     */
    public RedisBF(boolean initByDefault) {
        this(DEFAULT_CAPACITY, DEFAULT_FPP, DEFAULT_EXPANSION, false);
    }

    /**
     * Constructs a new instance of RedisBF with specified parameters.
     *
     * @param initCapacity  the initial capacity for each Bloom Filter
     * @param initFpp       the false positive probability for each Bloom Filter
     * @param initExpansion the expansion factor for scaling the Bloom Filters
     * @param nonScaling    whether the Bloom Filter is non-scaling
     */
    public RedisBF(int initCapacity, double initFpp, byte initExpansion, boolean nonScaling) {
        this.fpp = initFpp;
        this.expansion = initExpansion;
        this.nonScaling = nonScaling;

        // add first one
        var one = new One();
        one.filter = BloomFilter.create(STRING_FUNNEL, initCapacity, initFpp);
        one.capacity = initCapacity;
        list.add(one);
    }

    /**
     * Returns the total number of items inserted across all Bloom Filters.
     *
     * @return the total number of items inserted
     */
    public int itemInserted() {
        return list.stream().mapToInt(one -> one.itemInserted).sum();
    }

    /**
     * Returns the total capacity across all Bloom Filters.
     *
     * @return the total capacity
     */
    public int capacity() {
        return list.stream().mapToInt(one -> one.capacity).sum();
    }

    /**
     * Returns the number of Bloom Filters in the list.
     *
     * @return the size of the list
     */
    public int listSize() {
        return list.size();
    }

    /**
     * Estimates the memory allocated by the Bloom Filters.
     *
     * @return the estimated memory allocation (not yet implemented)
     */
    public int memoryAllocatedEstimate() {
        // todo
        return 0;
    }

    /**
     * Checks if an item might be in any of the Bloom Filters.
     *
     * @param item the item to check
     * @return true if the item might be in any of the Bloom Filters, false otherwise
     */
    public boolean mightContain(String item) {
        return list.stream().anyMatch(one -> one.filter.mightContain(item));
    }

    /**
     * Adds an item to the Bloom Filters.
     *
     * @param item the item to add
     * @return true if the item was added, false if it was already present in any of the Bloom Filters
     * @throws RuntimeException if the Bloom Filters cannot be expanded or if the list size is too large
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

        // need expansion
        if (nonScaling) {
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

    /**
     * Encodes the Bloom Filters to a byte array.
     *
     * @return the encoded byte array
     */
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
     * Decodes a byte array to a RedisBF object.
     *
     * @param data the byte array to decode
     * @return the RedisBF object
     */
    public static RedisBF decode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);

        var r = new RedisBF();
        r.expansion = buffer.get();
        r.nonScaling = buffer.get() != 0;
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