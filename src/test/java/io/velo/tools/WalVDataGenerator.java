package io.velo.tools;

import io.velo.CompressedValue;
import io.velo.KeyHash;
import io.velo.persist.Wal;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public interface WalVDataGenerator {

    default long n() {
        return 1_000_000L;
    }

    default String key(long i, int keyLength, String keyPrefix) {
        if (keyPrefix == null) {
            keyPrefix = "key:";
        }
        var seq = String.valueOf(i);
        var leftLength = keyLength - keyPrefix.length() - seq.length();
        if (leftLength <= 0) {
            throw new IllegalArgumentException("Key length must be greater than key prefix length.");
        }

        // padding left with 0
        var sb = new StringBuilder(keyPrefix);
        for (int j = 0; j < leftLength; j++) {
            sb.append('0');
        }
        sb.append(seq);
        return sb.toString();
    }

    long nextId();

    default Wal.V keyToV(long seq, int bucketIndex, String key, long keyHash) {
        var dataBytes = new byte[10];
        var cvEncoded = CompressedValue.encodeAsShortString(seq, dataBytes);
        return new Wal.V(nextId(), bucketIndex, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                key, cvEncoded, false);
    }

    default Map<Integer, List<Wal.V>> generateVListByWalGroupIndex(int oneChargeBucketNumber, int keyLength, String keyPrefix) {
        var n = n();
        ArrayList<Wal.V> list = new ArrayList<>();
        var beginTime = System.currentTimeMillis();
        for (long i = 0; i < n; i++) {
            var key = key(i, keyLength, keyPrefix);
            var keyHash = KeyHash.hash(key.getBytes());
            var bucketIndex = KeyHash.bucketIndex(keyHash);
            list.add(keyToV(nextId(), bucketIndex, key, keyHash));
        }
        var costTime = System.currentTimeMillis() - beginTime;
        System.out.println("Generate key list cost time=" + costTime + " ms");

        // group by bucket index
        var beginTime2 = System.currentTimeMillis();
        var map = list.stream().collect(Collectors.groupingBy(one -> one.bucketIndex() / oneChargeBucketNumber));
        var costTime2 = System.currentTimeMillis() - beginTime2;
        System.out.println("Group by wal group index cost time=" + costTime2 + " ms");

        return map;
    }
}
