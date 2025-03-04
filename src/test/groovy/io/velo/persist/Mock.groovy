package io.velo.persist

import io.netty.buffer.Unpooled
import io.velo.CompressedValue
import io.velo.KeyHash

import java.util.function.Function

class Mock {
    static List<String> prepareTargetBucketIndexKeyList(int n, int bucketIndex = 0) {
        List<String> targetBucketIndexKeyList = []
        n.times {
            def rawKey = 'key:' + it.toString().padLeft(12, '0')
            targetBucketIndexKeyList << ('xh!' + bucketIndex + '_' + rawKey)
        }
        targetBucketIndexKeyList
    }

    static List<Wal.V> prepareShortValueList(int n, int bucketIndex = 0, Function<Wal.V, Wal.V> transfer = null) {
        List<Wal.V> shortValueList = []
        n.times {
            def key = 'key:' + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes
            def cv = new CompressedValue()
            cv.compressedData = ('value' + it).bytes
            def cvEncoded = cv.encodeAsShortString()

            def keyHash = KeyHash.hash(keyBytes)

            def v = new Wal.V(it, bucketIndex, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                    key, cvEncoded, false)
            if (transfer != null) {
                v = transfer.apply(v)
            }

            shortValueList << v
        }
        shortValueList
    }

    static CompressedValue fromV(Wal.V v) {
        return CompressedValue.decode(Unpooled.wrappedBuffer(v.cvEncoded()), v.key().bytes, v.keyHash())
    }

    static ArrayList<Wal.V> prepareValueList(int n, int bucketIndex = 0, Function<Wal.V, Wal.V> transfer = null) {
        ArrayList<Wal.V> valueList = []
        n.times {
            def key = 'key:' + it.toString().padLeft(12, '0')
            def keyBytes = key.bytes

            def keyHash = KeyHash.hash(keyBytes)

            def cv = new CompressedValue()
            cv.seq = it
            cv.dictSeqOrSpType = 1
            cv.keyHash = keyHash
            cv.compressedData = new byte[10]

            def v = new Wal.V(it, bucketIndex, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                    key, cv.encode(), false)
            if (transfer != null) {
                v = transfer.apply(v)
            }

            valueList << v
        }
        valueList
    }

    static List<CompressedValue> prepareCompressedValueList(int n) {
        List<CompressedValue> compressedValueList = []
        n.times {
            def cv = new CompressedValue()
            cv.seq = it
            cv.keyHash = it
            cv.compressedData = new byte[10]
            compressedValueList << cv
        }
        compressedValueList
    }

    static byte[] prepareShortStringCvEncoded(String key, String value) {
        def cv = new CompressedValue()
        cv.keyHash = KeyHash.hash(key.bytes)
        cv.compressedData = value.bytes
        cv.encodeAsShortString()
    }

    // tuple2: key, keyHash
    static Map<Integer, List<Tuple2<String, Long>>> prepareKeyHashIndexByKeyBucketList(int n, int bucketsPerSlot) {
        Map<Integer, List<Tuple2<String, Long>>> keyHashByBucketIndex = [:]
        n.times {
            def key = 'key:' + it.toString().padLeft(12, '0')
            def keyHash = KeyHash.hash(key.bytes)
            def bucketIndex = (int) Math.abs((keyHash & (bucketsPerSlot - 1)).intValue())
            def subList = keyHashByBucketIndex[bucketIndex]
            if (subList == null) {
                subList = []
                keyHashByBucketIndex[bucketIndex] = subList
            }
            subList << new Tuple2(key, keyHash)
        }
        keyHashByBucketIndex
    }

    static List<String> prepareToClientSlotKeyList(short n, short toClientSlot) {
        List<String> list = (0..<n).collect { i ->
            // use hash tag
            'key:' + i.toString().padLeft(12, '0') +
                    '_{' + MockForCluster.toClientSlotPreparedKeys[toClientSlot] + '}'
        }
        list
    }
}
