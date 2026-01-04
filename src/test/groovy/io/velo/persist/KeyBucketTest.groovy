package io.velo.persist

import io.velo.CompressedValue
import io.velo.KeyHash
import io.velo.SnowFlake
import org.jetbrains.annotations.NotNull
import spock.lang.Specification

import java.nio.ByteBuffer

class KeyBucketTest extends Specification {
    final short slot = 0

    def 'test base'() {
        given:
        def kvMeta = new KeyBucket.KVMeta(0, (short) 16, (byte) 24)
        def kvMeta2 = new KeyBucket.KVMeta(0, (short) 16, (byte) 101)
        println kvMeta

        def x = new KeyBucket.ValueBytesWithExpireAtAndSeq('a'.bytes, 0L, 97L)
        def x2 = new KeyBucket.ValueBytesWithExpireAtAndSeq('a'.bytes, System.currentTimeMillis() - 1000, 97L)
        def x3 = new KeyBucket.ValueBytesWithExpireAtAndSeq('a'.bytes, System.currentTimeMillis() + 1000, 97L)
        def y = new KeyBucket.ExpireAtAndSeq(0L, 97L)
        def y2 = new KeyBucket.ExpireAtAndSeq(System.currentTimeMillis() - 1000, 97L)
        def y3 = new KeyBucket.ExpireAtAndSeq(System.currentTimeMillis() + 1000, 97L)

        expect:
        KeyBucket.KVMeta.calcCellCount((short) 16, (byte) 24) == 1
        KeyBucket.KVMeta.calcCellCount((short) 16, (byte) 41) == 1
        KeyBucket.KVMeta.calcCellCount((short) 16, (byte) 42) == 2

        kvMeta.cellCount() == 1
        kvMeta2.cellCount() == 2

        !x.isExpired()
        x2.isExpired()
        !x3.isExpired()
        !y.isExpired()
        y2.isExpired()
        !y3.isExpired()

        when:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)
        def expireAtAndSeq = keyBucket.getExpireAtAndSeqByKey('a', 97L)
        def valueX = keyBucket.getValueXByKey('a', 97L)
        then:
        expireAtAndSeq == null
        valueX == null

        when:
        keyBucket.put('b', 98L, 0L, 2L, 'b'.bytes)
        expireAtAndSeq = keyBucket.getExpireAtAndSeqByKey('a', 97L)
        valueX = keyBucket.getValueXByKey('a', 97L)
        then:
        expireAtAndSeq == null
        valueX == null

        when:
        keyBucket.put('a', 97L, 0L, 1L, 'a'.bytes)
        expireAtAndSeq = keyBucket.getExpireAtAndSeqByKey('a', 97L)
        valueX = keyBucket.getValueXByKey('a', 97L)
        then:
        expireAtAndSeq.expireAt() == 0L
        expireAtAndSeq.seq() == 1L
        valueX.expireAt() == 0L
        valueX.seq() == 1L
        valueX.valueBytes() == 'a'.bytes
    }

    def 'del then put corner case'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        keyBucket.put('a', 97L, 0L, 1L, 'a'.bytes)
        keyBucket.put('b', 98L, 0L, 2L, 'b'.bytes)
        keyBucket.put('c', 99L, 0L, 3L, new PersistValueMeta().encode())
        println keyBucket
        keyBucket.allPrint()
        keyBucket.putMeta()
        then:
        keyBucket.getSplitIndex() == 0
        keyBucket.size == 3
        keyBucket.del('a', 97L, true)
        keyBucket.size == 2

        when:
        keyBucket.put('b', 98L, 0L, 2L, 'bb'.bytes)
        then:
        keyBucket.size == 2

        when:
        keyBucket.put('c', 99L, 0L, 3L, 'cc'.bytes)
        then:
        keyBucket.size == 2
    }

    def 'shared bytes'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        def k1 = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)
        def k2 = new KeyBucket(slot, 1, (byte) 0, (byte) 1, null, snowFlake)
        def k3 = new KeyBucket(slot, 1, (byte) 0, (byte) 1, new byte[4096 * 2], 4096, snowFlake)

        and:
        k1.put('a', 97L, 0L, 1L, 'a'.bytes)
        def k1Bytes = k1.encode(false)

        k2.put('a', 97L, 0L, 1L, 'a'.bytes)
        def k2Bytes = k2.encode(false)

        def k3Bytes = k3.encode(false)

        def sharedBytes = new byte[4096 * 2]
        System.arraycopy(k1Bytes, 0, sharedBytes, 0, k1Bytes.length)
        System.arraycopy(k2Bytes, 0, sharedBytes, 4096, k2Bytes.length)

        when:
        def k11 = new KeyBucket(slot, 0, (byte) 0, (byte) 1, sharedBytes, 0, snowFlake)
        def k22 = new KeyBucket(slot, 1, (byte) 0, (byte) 1, sharedBytes, 4096, snowFlake)
        def k33 = new KeyBucket(slot, 1, (byte) 0, (byte) 1, sharedBytes, sharedBytes.length, snowFlake)
        def k333 = new KeyBucket(slot, 1, (byte) 0, (byte) -1, sharedBytes, sharedBytes.length, snowFlake)
        def isInvalidBytes = false
        KeyBucket k44
        try {
            k44 = new KeyBucket(slot, 1, (byte) 0, (byte) 1, new byte[5000], 0, snowFlake)
        } catch (IllegalStateException e) {
            println e.message
            isInvalidBytes = true
        }
        def isInvalidBytes2 = false
        def invalidBytes2 = new byte[4096]
        def buffer55 = ByteBuffer.wrap(invalidBytes2)
        buffer55.putLong(8L)

        KeyBucket k55
        try {
            k55 = new KeyBucket(slot, 1, (byte) 0, (byte) 1, invalidBytes2, 0, snowFlake)
        } catch (IllegalStateException e) {
            println e.message
            isInvalidBytes2 = true
        }
        then:
        k3Bytes.length == 4096
        k11.isSharedBytes()
        k22.isSharedBytes()
        !k33.isSharedBytes()
        k11.size == 1
        k22.size == 1
        k33.size == 0
        isInvalidBytes
        isInvalidBytes2
        k11.getValueXByKey('a', 97L).valueBytes() == 'a'.bytes
        k22.getValueXByKey('a', 97L).valueBytes() == 'a'.bytes
    }

    def 'multi cell count'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        keyBucket.put('a', 97L, 0L, 1L, 'a'.bytes)
        then:
        keyBucket.size == 1
        keyBucket.cellCost == 1

        when:
        // 2 + 90 + 1 + 6 = 99
        // 99 / 54 = 1, cell count = 3
        def longKey = 'a'.padRight(90, 'a')
        keyBucket.put(longKey, 9797L, 0L, 1L, 'long a'.bytes)
        then:
        keyBucket.size == 2
        keyBucket.cellCost == 3
        keyBucket.getValueXByKey(longKey, 9797L).valueBytes() == 'long a'.bytes

        when:
        keyBucket.put('bb', 9898L, System.currentTimeMillis() + 1000, 22L, 'bb'.bytes)
        keyBucket.put('b', 98L, System.currentTimeMillis() - 1000, 2L, 'b'.bytes)
        then:
        keyBucket.size == 4
        keyBucket.getValueXByKey('b', 98L) != null
        keyBucket.getValueXByKey('bb', 9898L) != null

        when:
        def longKeyString = 'long-key' * 8
        keyBucket.put(longKeyString, 100L, System.currentTimeMillis() - 1000, 100L, 'long-value'.bytes)
        keyBucket.clearAllExpired()
        keyBucket.iterate { keyHash, expireAt, seq, key, valueBytes ->
            println key + ': ' + new String(valueBytes)
        }
        then:
        keyBucket.size == 3
        keyBucket.getValueXByKey('b', 98L) == null
        keyBucket.getValueXByKey('bb', 9898L) != null

        when:
        keyBucket.clearAll()
        then:
        keyBucket.size == 0
        keyBucket.cellCost == 0
        keyBucket.getValueXByKey('a', 97L) == null
        keyBucket.getValueXByKey(longKey, 9797L) == null
    }

    def 'test last update seq'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        keyBucket.updateSeq()
        def lastUpdateSplitNumber = (byte) (keyBucket.lastUpdateSeq & 0b1111)
        then:
        lastUpdateSplitNumber == 1

        when:
        keyBucket.splitNumber = 3
        keyBucket.updateSeq()
        lastUpdateSplitNumber = (byte) (keyBucket.lastUpdateSeq & 0b1111)
        then:
        lastUpdateSplitNumber == 3

        when:
        def encoded = keyBucket.encode(true)
        def keyBucket2 = new KeyBucket(slot, 0, (byte) 0, (byte) -1, encoded, snowFlake)
        then:
        keyBucket2.splitNumber == 3
    }

    def 'test hash conflict'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        keyBucket.put('a', 97L, 0L, 1L, 'a'.bytes)
        keyBucket.put('aa', 97L, 0L, 1L, 'aa'.bytes)
        keyBucket.put('ax', 97L, 0L, 1L, 'ax'.bytes)
        then:
        keyBucket.size == 3
        keyBucket.getValueXByKey('a', 97L).valueBytes() == 'a'.bytes
        keyBucket.getValueXByKey('aa', 97L).valueBytes() == 'aa'.bytes
        keyBucket.getValueXByKey('ax', 97L).valueBytes() == 'ax'.bytes
    }

    def 'test put full'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        (1..KeyBucket.INIT_CAPACITY).each {
            keyBucket.put('key:' + it, it, 0L, it, 'value'.bytes)
        }
        then:
        keyBucket.size == KeyBucket.INIT_CAPACITY

        when:
        def r = keyBucket.put('key:' + (KeyBucket.INIT_CAPACITY + 1), KeyBucket.INIT_CAPACITY + 1, 0L, KeyBucket.INIT_CAPACITY + 1, 'value'.bytes)
        then:
        !r.isPut()
        !r.isUpdate()

        when:
        boolean exception = false
        def longValueString = 'value' * 30
        try {
            keyBucket.put('long-key', 100L, 0L, 100L, longValueString.bytes)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def longKeyString = 'long-key' * 1000
        try {
            keyBucket.put(longKeyString, 100L, 0L, 100L, 'value'.bytes)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            keyBucket.clearOneExpiredOrDeleted(KeyBucket.INIT_CAPACITY)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test put cell reuse'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        keyBucket.put('a', 97L, 0L, 1L, 'a'.bytes)
        def cell2Key = 'abcd' * 4
        def cell2ValueBytes = new byte[42]
        keyBucket.put(cell2Key, 100L, System.currentTimeMillis() - 1000, 100L, cell2ValueBytes)
        then:
        keyBucket.size == 2
        keyBucket.cellCost == 3

        when:
        keyBucket.put('b', 98L, 0L, 98L, 'b'.bytes)
        then:
        keyBucket.size == 2
        keyBucket.cellCost == 2

        when:
        keyBucket.put(cell2Key, 100L, 0L, 100L, cell2ValueBytes)
        then:
        keyBucket.size == 3
        keyBucket.cellCost == 4

        when:
        // overwrite key but only one cell cost
        def cell1Key = cell2Key
        def cell1ValueBytes = new byte[41]
        keyBucket.put(cell1Key, 100L, 0L, 100L, cell1ValueBytes)
        then:
        keyBucket.size == 3
        keyBucket.cellCost == 3
    }

    def 'test cell available check'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)

        expect:
        keyBucket.isCellAvailableN(0, KeyBucket.INIT_CAPACITY, false)
        keyBucket.isCellAvailableN(0, KeyBucket.INIT_CAPACITY, true)
        !keyBucket.isCellAvailableN(0, KeyBucket.INIT_CAPACITY + 1, true)

        when:
        keyBucket.put('a', 97L, 0L, 1L, 'a'.bytes)
        then:
        !keyBucket.isCellAvailableN(0, 1, false)
        !keyBucket.isCellAvailableN(0, 1, true)
        keyBucket.isCellAvailableN(1, KeyBucket.INIT_CAPACITY - 1, false)

        when:
        def cell2Key = 'abcd' * 4
        def cell2ValueBytes = new byte[42]
        keyBucket.put(cell2Key, 100L, 0L, 100L, cell2ValueBytes)
        then:
        !keyBucket.isCellAvailableN(1, 1, true)
        !keyBucket.isCellAvailableN(1, 1, false)
        !keyBucket.isCellAvailableN(2, 1, false)
        keyBucket.isCellAvailableN(2, 1, true)
    }

    def 'test re put all'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def keyBucket = new KeyBucket(slot, 0, (byte) 0, (byte) 1, null, snowFlake)

        when:
        keyBucket.put('a', 97L, 0L, 97L, 'a'.bytes)
        keyBucket.put('b', 98L, 0L, 98L, 'b'.bytes)
        keyBucket.put('c', 99L, 0L, 99L, 'c'.bytes)
        keyBucket.del('b', 98L, false)
        keyBucket.rePutAll()
        then:
        keyBucket.size == 2

        when:
        keyBucket.clearAll()
        (1..KeyBucket.INIT_CAPACITY).each {
            keyBucket.put('key:' + it, it, 0L, it, 'value'.bytes)
        }
        keyBucket.del('key:1', 1, false)
        keyBucket.del('key:3', 3, false)
        // add cost two cells key value pair
        def cell2Key = 'abcd' * 4
        def cell2Cv = new CompressedValue()
        cell2Cv.keyHash = KeyHash.hash(cell2Key.bytes)
        cell2Cv.compressedData = new byte[30]
        // will trigger re-put all
        keyBucket.put(cell2Key, 100L, 0, 100L, cell2Cv.encode())
        then:
        keyBucket.size == KeyBucket.INIT_CAPACITY - 1
        keyBucket.cellCost == KeyBucket.INIT_CAPACITY

        when:
        keyBucket.cvExpiredOrDeletedCallBack = new KeyBucket.CvExpiredOrDeletedCallBack() {
            @Override
            void handle(@NotNull String key, @NotNull CompressedValue shortStringCv) {
                println key + ' with short string cv expired'
            }

            @Override
            void handle(@NotNull String key, @NotNull PersistValueMeta pvm) {
                println key + ' with pvm expired'
            }
        }
        keyBucket.del(cell2Key, 100L, false)
        // put again
        keyBucket.put(cell2Key, 100L, 0, 100L, cell2Cv.encode())
        keyBucket.clearOneExpiredOrDeleted(KeyBucket.INIT_CAPACITY - 2)
        then:
        keyBucket.size == KeyBucket.INIT_CAPACITY - 2
        keyBucket.cellCost == KeyBucket.INIT_CAPACITY - 2

        when:
        def pvm = new PersistValueMeta()
        keyBucket.put('normal-key', 100L, 0, 100L, pvm.encode())
        keyBucket.clearOneExpiredOrDeleted(KeyBucket.INIT_CAPACITY - 2)
        then:
        keyBucket.size == KeyBucket.INIT_CAPACITY - 2
    }
}
