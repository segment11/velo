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

public class RedisZSet {
    // change here to limit zset size
    // values encoded compressed length should <= 4KB, suppose ratio is 0.25, then 16KB
    // suppose value length is 32, then 16KB / 32 = 512
    public static short ZSET_MAX_SIZE = 4096;

    public static short ZSET_MEMBER_MAX_LENGTH = 255;

    @VisibleForTesting
    // size short + dict seq int + body bytes length int + crc int
    static final int HEADER_LENGTH = 2 + 4 + 4 + 4;

    public static final String MEMBER_MAX = "+";
    public static final String MEMBER_MIN = "-";

    public static class ScoreValue implements Comparable<ScoreValue> {
        private double score;
        private final String member;

        public ScoreValue(double score, @NotNull String member) {
            this.score = score;
            this.member = member;
        }

        private int initRank = 0;

        public int getInitRank() {
            return initRank;
        }

        public void setInitRank(int initRank) {
            this.initRank = initRank;
        }

        public boolean isAlreadyWeighted = false;

        public double score() {
            return score;
        }

        public void score(double score) {
            this.score = score;
        }

        public String member() {
            return member;
        }

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

        public int length() {
            // score double + value length short
            return 8 + member.length();
        }
    }

    private final TreeSet<ScoreValue> set = new TreeSet<>();
    private final TreeMap<String, ScoreValue> memberMap = new TreeMap<>();

    // need not thread safe
    public TreeSet<ScoreValue> getSet() {
        return set;
    }

    public TreeMap<String, ScoreValue> getMemberMap() {
        return memberMap;
    }

    private static final double addFixed = 0.0000000000001;

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

    public int size() {
        return memberMap.size();
    }

    public boolean isEmpty() {
        return memberMap.isEmpty();
    }

    public boolean contains(String member) {
        return memberMap.containsKey(member);
    }

    public boolean remove(String member) {
        var sv = memberMap.get(member);
        if (sv == null) {
            return false;
        }
        memberMap.remove(member);
        return set.remove(sv);
    }

    public void clear() {
        set.clear();
        memberMap.clear();
    }

    public ScoreValue pollFirst() {
        var sv = set.pollFirst();
        if (sv == null) {
            return null;
        }
        memberMap.remove(sv.member);
        return sv;
    }

    public ScoreValue pollLast() {
        var sv = set.pollLast();
        if (sv == null) {
            return null;
        }
        memberMap.remove(sv.member);
        return sv;
    }

    public boolean add(double score, String member) {
        return add(score, member, true, false);
    }

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

    public ScoreValue get(String member) {
        return memberMap.get(member);
    }

    public void print() {
        for (var member : set) {
            System.out.println(member);
        }
    }

    public byte[] encodeButDoNotCompress() {
        return encode(null);
    }

    public byte[] encode() {
        return encode(Dict.SELF_ZSTD_DICT);
    }

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
            buffer.putDouble(e.score);
            buffer.put(e.member.getBytes());
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

    public static int getSizeWithoutDecode(byte[] data) {
        var buffer = ByteBuffer.wrap(data);
        return buffer.getShort();
    }

    public static RedisZSet decode(byte[] data) {
        return decode(data, true);
    }

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
                throw new IllegalStateException("Crc check failed");
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
