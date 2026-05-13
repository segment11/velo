package io.velo.type;

import io.velo.Dict;
import io.velo.DictMap;
import io.velo.KeyHash;
import io.velo.persist.Wal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Sorted set with Zstd compression support.
 */
public class RedisZSet {
    /**
     * Maximum zset size (4096).
     * Encoded and compressed length should be under 4KB.
     */
    public static short ZSET_MAX_SIZE = 4096;

    /** Maximum length of a zset member. */
    public static short ZSET_MEMBER_MAX_LENGTH = 255;

    /** Header length: size short + dict seq int + body length int + crc int */
    @VisibleForTesting
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    /** Maximum member value for range queries. */
    public static final String MEMBER_MAX = "+";

    /** Minimum member value for range queries. */
    public static final String MEMBER_MIN = "-";

    /**
     * Score-value pair for sorted set.
     */
    public static class ScoreValue implements Comparable<ScoreValue> {
        private double score;
        private final String member;

        /**
         * @param score  the score
         * @param member the member
         */
        public ScoreValue(double score, @NotNull String member) {
            this.score = score;
            this.member = member;
        }

        private int initRank = 0;

        /** @return initial rank */
        public int getInitRank() {
            return initRank;
        }

        /**
         * @param initRank the initial rank to set
         */
        public void setInitRank(int initRank) {
            this.initRank = initRank;
        }

        /** Whether already weighted. */
        public boolean isAlreadyWeighted = false;

        /** @return the score */
        public double score() {
            return score;
        }

        /**
         * @param score the score to set
         */
        public void score(double score) {
            this.score = score;
        }

        /** @return the member */
        public String member() {
            return member;
        }

        /**
         * @param o the other ScoreValue to compare
         * @return comparison result
         */
        @Override
        public int compareTo(@NotNull RedisZSet.ScoreValue o) {
            if (score == o.score) {
                return member.compareTo(o.member);
            }
            return Double.compare(score, o.score);
        }

        @Override
        public String toString() {
            return "ScoreValue{" +
                    "score=" + score +
                    ", member='" + member + '\'' +
                    '}';
        }

        /**
         * @return length in bytes (8 for score + key bytes length)
         */
        public int length() {
            return 8 + Wal.keyBytes(member).length;
        }
    }

    private final TreeSet<ScoreValue> set = new TreeSet<>();

    private final TreeMap<String, ScoreValue> memberMap = new TreeMap<>();

    /** @return internal sorted set of score-value pairs */
    public TreeSet<ScoreValue> getSet() {
        return set;
    }

    /** @return internal map of members to score-value pairs */
    public TreeMap<String, ScoreValue> getMemberMap() {
        return memberMap;
    }

    private static final double addFixed = 0.0000000000001;

    /**
     * @param min          minimum score
     * @param minInclusive whether minimum is inclusive
     * @param max          maximum score
     * @param maxInclusive whether maximum is inclusive
     * @return navigable set of score-value pairs in range
     */
    public NavigableSet<ScoreValue> between(double min, boolean minInclusive,
                                           double max, boolean maxInclusive) {
        double minFixed = min != Double.NEGATIVE_INFINITY ? min - addFixed : Double.NEGATIVE_INFINITY;
        double maxFixed = max != Double.POSITIVE_INFINITY ? max + addFixed : Double.POSITIVE_INFINITY;

        var subSet = set.subSet(new ScoreValue(minFixed, ""), false,
                new ScoreValue(maxFixed, ""), false);
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
     * @param min          minimum member
     * @param minInclusive whether minimum is inclusive
     * @param max          maximum member
     * @param maxInclusive whether maximum is inclusive
     * @return navigable map of score-value pairs in range
     */
    public NavigableMap<String, ScoreValue> betweenByMember(String min, boolean minInclusive,
                                                           String max, boolean maxInclusive) {
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
        return new TreeMap<>(subMap);
    }

    /** @return number of score-value pairs */
    public int size() {
        return memberMap.size();
    }

    /** @return true if empty */
    public boolean isEmpty() {
        return memberMap.isEmpty();
    }

    /**
     * @param member the member to check
     * @return true if contained
     */
    public boolean contains(String member) {
        return memberMap.containsKey(member);
    }

    /**
     * @param member the member to remove
     * @return true if removed
     */
    public boolean remove(String member) {
        var sv = memberMap.get(member);
        if (sv == null) {
            return false;
        }
        memberMap.remove(member);
        return set.remove(sv);
    }

    /** Clears all score-value pairs. */
    public void clear() {
        set.clear();
        memberMap.clear();
    }

    /**
     * @return score-value pair with lowest score or null if empty
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
     * @return score-value pair with highest score or null if empty
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
     * @param score  the score
     * @param member the member
     * @return true if added
     */
    public boolean add(double score, String member) {
        return add(score, member, true, false);
    }

    /**
     * @param score             the score
     * @param member            the member
     * @param overwrite         whether to overwrite existing
     * @param isAlreadyWeighted whether score is already weighted
     * @return true if added
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
     * @param member the member to retrieve
     * @return score-value pair or null if not found
     */
    public ScoreValue get(String member) {
        return memberMap.get(member);
    }

    /** Prints all score-value pairs. */
    public void print() {
        for (var member : set) {
            System.out.println(member);
        }
    }

    /**
     * @return encoded byte array without compression
     */
    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    /**
     * @return encoded and compressed byte array
     */
    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

    /**
     * @param dict compression dictionary or null
     * @return encoded byte array, possibly compressed
     */
    public byte[] encode(Dict dict) {
        int bodyBytesLength = 0;
        for (var member : set) {
            bodyBytesLength += 2 + member.length();
        }

        int size = set.size();
        if (size > Short.MAX_VALUE) {
            throw new IllegalStateException("ZSet size " + size + " exceeds Short.MAX_VALUE");
        }

        var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
        buffer.putShort((short) size);
        buffer.putInt(0);
        buffer.putInt(bodyBytesLength);
        buffer.putInt(0);
        for (var e : set) {
            var memberBytes = Wal.keyBytes(e.member());
            buffer.putShort((short) memberBytes.length);
            buffer.putDouble(e.score());
            buffer.put(memberBytes);
        }

        int crc = 0;
        if (bodyBytesLength > 0) {
            var hb = buffer.array();
            crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(HEADER_LENGTH - 4, crc);
        }

        var rawBytesWithHeader = buffer.array();
        if (bodyBytesLength > DictMap.TO_COMPRESS_MIN_DATA_LENGTH && dict != null) {
            var compressedBytes = RedisHH.compressIfBytesLengthIsLong(
                    dict, bodyBytesLength, rawBytesWithHeader, (short) size, crc);
            if (compressedBytes != null) {
                return compressedBytes;
            }
        }
        return rawBytesWithHeader;
    }

    /**
     * @param data the encoded byte array
     * @return size without decoding entire array
     */
    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    /**
     * @param data the byte array to decode
     * @return decoded RedisZSet
     */
    public static RedisZSet decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * @param data         the byte array to decode
     * @param doCheckCrc32 whether to check CRC32
     * @return decoded RedisZSet
     */
    public static RedisZSet decode(byte[] data, boolean doCheckCrc32) {
        var buffer = ByteBuffer.wrap(data);
        int size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            buffer = RedisHH.decompressIfUseDict(dictSeq, bodyBytesLength, data);
        }

        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(
                    buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("CRC check failed");
            }
        }

        var r = new RedisZSet();
        int rank = 0;
        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            if (len <= 0) {
                throw new IllegalStateException(
                        "Invalid zset member length: " + len + ", expected > 0");
            }
            if (len + 8 > buffer.remaining()) {
                throw new IllegalStateException(
                        "Invalid zset member length: " + len + ", exceeds remaining buffer");
            }
            double score = buffer.getDouble();
            var bytes = new byte[len];
            buffer.get(bytes);
            var member = Wal.keyString(bytes);
            var sv = new ScoreValue(score, member);

            sv.setInitRank(rank);
            rank++;

            r.set.add(sv);
            r.memberMap.put(member, sv);
        }
        return r;
    }

    /**
     * Callback for iterating over encoded members.
     */
    public interface IterateCallback {
        /**
         * @param memberBytes the member bytes
         * @param score       the score
         * @param rank        the rank
         * @return true to break iteration
         */
        boolean on(byte[] memberBytes, double score, int rank);
    }

    /**
     * @param data         the byte array to iterate
     * @param doCheckCrc32 whether to check CRC32
     * @param callback     callback for each member
     */
    public static void iterate(byte[] data, boolean doCheckCrc32, IterateCallback callback) {
        var buffer = ByteBuffer.wrap(data);
        int size = buffer.getShort();
        var dictSeq = buffer.getInt();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (dictSeq > 0) {
            buffer = RedisHH.decompressIfUseDict(dictSeq, bodyBytesLength, data);
        }

        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(
                    buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("CRC check failed");
            }
        }

        for (int i = 0; i < size; i++) {
            int len = buffer.getShort();
            if (len <= 0) {
                throw new IllegalStateException(
                        "Invalid zset member length: " + len + ", expected > 0");
            }
            if (len + 8 > buffer.remaining()) {
                throw new IllegalStateException(
                        "Invalid zset member length: " + len + ", exceeds remaining buffer");
            }
            double score = buffer.getDouble();
            var bytes = new byte[len];
            buffer.get(bytes);

            var isBreak = callback.on(bytes, score, i);
            if (isBreak) {
                break;
            }
        }
    }
}
