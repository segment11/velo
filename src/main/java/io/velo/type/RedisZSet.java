package io.velo.type;

import io.velo.Dict;
import io.velo.DictMap;
import io.velo.KeyHash;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * A class representing a sorted set (zset) that can be encoded and decoded with optional compression using Zstandard.
 * This class is designed to handle a sorted set of members with associated scores and provides methods for encoding and decoding the data.
 */
public class RedisZSet {
    /**
     * The maximum size of the zset. This is set to 4096.
     * This value can be changed by configuration.
     * The values encoded and compressed length should be less than or equal to 4KB, assuming a compression ratio of 0.25, then 16KB.
     * Assuming a value length of 32, then 16KB / 32 = 512.
     */
    public static short ZSET_MAX_SIZE = 4096;

    /**
     * The maximum length of a zset member. This is set to 255.
     */
    public static short ZSET_MEMBER_MAX_LENGTH = 255;

    /**
     * The length of the header in bytes
     * size short + dict seq int + body length int + crc int
     */
    @VisibleForTesting
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    /**
     * A constant representing the maximum member value for range queries.
     */
    public static final String MEMBER_MAX = "+";

    /**
     * A constant representing the minimum member value for range queries.
     */
    public static final String MEMBER_MIN = "-";

    /**
     * A class representing a score-value pair in the zset. Each pair has a score and a member.
     * This class implements the Comparable interface to allow sorting based on score and member.
     */
    public static class ScoreValue implements Comparable<ScoreValue> {
        private double score;
        private final String member;

        /**
         * Constructs a new instance of ScoreValue with the specified score and member.
         *
         * @param score  The score of the pair.
         * @param member The member of the pair.
         */
        public ScoreValue(double score, @NotNull String member) {
            this.score = score;
            this.member = member;
        }

        /**
         * The initial rank of the score-value pair.
         */
        private int initRank = 0;

        /**
         * Returns the initial rank of the score-value pair.
         *
         * @return The initial rank.
         */
        public int getInitRank() {
            return initRank;
        }

        /**
         * Sets the initial rank of the score-value pair.
         *
         * @param initRank The initial rank to set.
         */
        public void setInitRank(int initRank) {
            this.initRank = initRank;
        }

        /**
         * A flag indicating whether the score-value pair is already weighted.
         */
        public boolean isAlreadyWeighted = false;

        /**
         * Returns the score of the score-value pair.
         *
         * @return The score.
         */
        public double score() {
            return score;
        }

        /**
         * Sets the score of the score-value pair.
         *
         * @param score The score to set.
         */
        public void score(double score) {
            this.score = score;
        }

        /**
         * Returns the member of the score-value pair.
         *
         * @return The member.
         */
        public String member() {
            return member;
        }

        /**
         * Compares this score-value pair with another based on score and member.
         *
         * @param o The other score-value pair to compare with.
         * @return A negative integer, zero, or a positive integer as this object is less than, equal to, or greater than the specified object.
         */
        @Override
        public int compareTo(@NotNull RedisZSet.ScoreValue o) {
            if (score == o.score) {
                return member.compareTo(o.member);
            }
            return Double.compare(score, o.score);
        }

        /**
         * Returns a string representation of the score-value pair.
         *
         * @return A string representation of the score-value pair.
         */
        @Override
        public String toString() {
            return "ScoreValue{" +
                    "score=" + score +
                    ", member='" + member + '\'' +
                    '}';
        }

        /**
         * Returns the length of the score-value pair in bytes.
         *
         * @return The length in bytes.
         */
        public int length() {
            // score double + value length short
            return 8 + member.length();
        }
    }

    /**
     * The internal set to store score-value pairs in sorted order.
     */
    private final TreeSet<ScoreValue> set = new TreeSet<>();

    /**
     * The internal map to store score-value pairs by member.
     */
    private final TreeMap<String, ScoreValue> memberMap = new TreeMap<>();

    /**
     * Returns the internal sorted set containing score-value pairs.
     *
     * @return The internal sorted set.
     */
    // need not thread safe
    public TreeSet<ScoreValue> getSet() {
        return set;
    }

    /**
     * Returns the internal map containing score-value pairs by member.
     *
     * @return The internal map.
     */
    public TreeMap<String, ScoreValue> getMemberMap() {
        return memberMap;
    }

    /**
     * A fixed value added or subtracted to adjust the boundaries for range queries.
     */
    private static final double addFixed = 0.0000000000001;

    /**
     * Retrieves a subset of score-value pairs between the specified scores.
     *
     * @param min          The minimum score.
     * @param minInclusive Whether the minimum score is inclusive.
     * @param max          The maximum score.
     * @param maxInclusive Whether the maximum score is inclusive.
     * @return A navigable set of score-value pairs within the specified range.
     */
    public NavigableSet<ScoreValue> between(double min, boolean minInclusive, double max, boolean maxInclusive) {
        double minFixed = min != Double.NEGATIVE_INFINITY ? min - addFixed : Double.NEGATIVE_INFINITY;
        double maxFixed = max != Double.POSITIVE_INFINITY ? max + addFixed : Double.POSITIVE_INFINITY;

        var subSet = set.subSet(new ScoreValue(minFixed, ""), false, new ScoreValue(maxFixed, ""), false);
        var copySet = new TreeSet<>(subSet);
        var itTmp = copySet.iterator();
        while (itTmp.hasNext()) {
            var sv = itTmp.next();
            if (sv.score() == min && !minInclusive) {
                itTmp.remove();
            }
            if (sv.score() == max && !maxInclusive) {
                itTmp.remove();
            }
        }
        return copySet;
    }

    /**
     * Retrieves a subset of score-value pairs between the specified members.
     *
     * @param min          The minimum member.
     * @param minInclusive Whether the minimum member is inclusive.
     * @param max          The maximum member.
     * @param maxInclusive Whether the maximum member is inclusive.
     * @return A navigable map of score-value pairs within the specified range.
     */
    public NavigableMap<String, ScoreValue> betweenByMember(String min, boolean minInclusive, String max, boolean maxInclusive) {
        if (memberMap.isEmpty()) {
            return memberMap;
        }

        if (MEMBER_MIN.equals(min)) {
            min = "";
            minInclusive = true;
        }
        if (MEMBER_MAX.equals(max)) {
            max = memberMap.lastKey();
            maxInclusive = true;
        }
        var subMap = memberMap.subMap(min, minInclusive, max, maxInclusive);
        // copy one
        return new TreeMap<>(subMap);
    }

    /**
     * Returns the number of score-value pairs in the zset.
     *
     * @return The size of the zset.
     */
    public int size() {
        return memberMap.size();
    }

    /**
     * Checks if the zset is empty.
     *
     * @return True if the zset is empty, false otherwise.
     */
    public boolean isEmpty() {
        return memberMap.isEmpty();
    }

    /**
     * Checks if the zset contains the specified member.
     *
     * @param member The member to check.
     * @return True if the member is contained in the zset, false otherwise.
     */
    public boolean contains(String member) {
        return memberMap.containsKey(member);
    }

    /**
     * Removes the specified member from the zset.
     *
     * @param member The member to remove.
     * @return True if the member was removed, false otherwise.
     */
    public boolean remove(String member) {
        var sv = memberMap.get(member);
        if (sv == null) {
            return false;
        }
        memberMap.remove(member);
        return set.remove(sv);
    }

    /**
     * Clears all score-value pairs from the zset.
     */
    public void clear() {
        set.clear();
        memberMap.clear();
    }

    /**
     * Removes and returns the score-value pair with the lowest score.
     *
     * @return The score-value pair with the lowest score, or null if the zset is empty.
     */
    public ScoreValue pollFirst() {
        var sv = set.pollFirst();
        if (sv == null) {
            return null;
        }
        memberMap.remove(sv.member);
        return sv;
    }

    /**
     * Removes and returns the score-value pair with the highest score.
     *
     * @return The score-value pair with the highest score, or null if the zset is empty.
     */
    public ScoreValue pollLast() {
        var sv = set.pollLast();
        if (sv == null) {
            return null;
        }
        memberMap.remove(sv.member);
        return sv;
    }

    /**
     * Adds a score-value pair to the zset.
     *
     * @param score  The score of the pair.
     * @param member The member of the pair.
     * @return True if the pair was added, false if the pair already exists and overwrite is false.
     */
    public boolean add(double score, String member) {
        return add(score, member, true, false);
    }

    /**
     * Adds a score-value pair to the zset with optional overwrite and weighted flags.
     *
     * @param score             The score of the pair.
     * @param member            The member of the pair.
     * @param overwrite         Whether to overwrite an existing pair.
     * @param isAlreadyWeighted Whether the score is already weighted.
     * @return True if the pair was added, false if the pair already exists and overwrite is false.
     */
    public boolean add(double score, String member, boolean overwrite, boolean isAlreadyWeighted) {
        var svExist = memberMap.get(member);
        if (svExist != null) {
            if (!overwrite) {
                return false;
            }

            memberMap.remove(member);
            set.remove(svExist);

            var sv = new ScoreValue(score, member);
            sv.isAlreadyWeighted = isAlreadyWeighted;
            set.add(sv);
            memberMap.put(member, sv);
        } else {
            var sv = new ScoreValue(score, member);
            sv.isAlreadyWeighted = isAlreadyWeighted;
            set.add(sv);
            memberMap.put(member, sv);
        }
        return true;
    }

    /**
     * Retrieves the score-value pair for the specified member.
     *
     * @param member The member to retrieve.
     * @return The score-value pair for the specified member, or null if the member does not exist.
     */
    public ScoreValue get(String member) {
        return memberMap.get(member);
    }

    /**
     * Prints the score-value pairs in the zset.
     */
    public void print() {
        for (var member : set) {
            System.out.println(member);
        }
    }

    /**
     * Encodes the zset to a byte array without compression.
     *
     * @return The encoded byte array.
     */
    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    /**
     * Encodes the zset to a byte array with compression using the default dictionary.
     *
     * @return The encoded and compressed byte array.
     */
    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

    /**
     * Encodes the zset to a byte array with optional compression using the specified dictionary.
     *
     * @param dict The dictionary to use for compression, or null if no compression is desired.
     * @return The encoded byte array, possibly compressed.
     */
    public byte[] encode(Dict dict) {
        int bodyBytesLength = 0;
        for (var member : set) {
            // zset value length use 2 bytes
            bodyBytesLength += 2 + member.length();
        }

        short size = (short) set.size();

        var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
        buffer.putShort(size);
        // tmp no dict seq
        buffer.putInt(0);
        buffer.putInt(bodyBytesLength);
        // tmp crc
        buffer.putInt(0);
        for (var e : set) {
            buffer.putShort((short) e.length());
            buffer.putDouble(e.score());
            buffer.put(e.member().getBytes());
        }

        // crc
        int crc = 0;
        if (bodyBytesLength > 0) {
            var hb = buffer.array();
            crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(HEADER_LENGTH - 4, crc);
        }

        var rawBytesWithHeader = buffer.array();
        if (bodyBytesLength > DictMap.TO_COMPRESS_MIN_DATA_LENGTH && dict != null) {
            var compressedBytes = RedisHH.compressIfBytesLengthIsLong(dict, bodyBytesLength, rawBytesWithHeader, size, crc);
            if (compressedBytes != null) {
                return compressedBytes;
            }
        }
        return rawBytesWithHeader;
    }

    /**
     * Retrieves the size of the zset without decoding the entire byte array.
     *
     * @param data The byte array containing the encoded zset.
     * @return The size of the zset.
     */
    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    /**
     * Decodes a byte array to a RedisZSet object. Checks the CRC32 by default.
     *
     * @param data The byte array to decode.
     * @return The RedisZSet object.
     */
    public static RedisZSet decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * Decodes a byte array to a RedisZSet object with optional CRC32 check.
     *
     * @param data         The byte array to decode.
     * @param doCheckCrc32 Whether to check the CRC32.
     * @return The RedisZSet object.
     */
    public static RedisZSet decode(byte[] data, boolean doCheckCrc32) {
        var buffer = ByteBuffer.wrap(data);
        int size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            // decompress first
            buffer = RedisHH.decompressIfUseDict(dictSeq, bodyBytesLength, data);
        }

        // check crc
        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("CRC check failed");
            }
        }

        var r = new RedisZSet();
        int rank = 0;
        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            double score = buffer.getDouble();
            var bytes = new byte[len - 8];
            buffer.get(bytes);
            var member = new String(bytes);
            var sv = new ScoreValue(score, member);

            sv.setInitRank(rank);
            rank++;

            r.set.add(sv);
            r.memberMap.put(member, sv);
        }
        return r;
    }
}