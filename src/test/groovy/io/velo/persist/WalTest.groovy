package io.velo.persist

import io.netty.buffer.Unpooled
import io.velo.*
import org.apache.commons.io.FileUtils
import spock.lang.Specification

import java.nio.ByteBuffer

class WalTest extends Specification {
    final short slot = 0

    def 'test base'() {
        given:
        def a = new Wal.V(1, 0, 0, 0, 0, 'a', 'a'.bytes, false)
        def b = new Wal.V(2, 0, 0, 0, 0, 'b', 'b'.bytes, false)
        def bb = new Wal.V(2, 0, 0, 0, 0, 'b', 'b'.bytes, false)

        expect:
        a < b
        b == bb
        b > a

        when:
        byte[] encoded = [CompressedValue.SP_FLAG_DELETE_TMP]
        def vRemoved = new Wal.V(1, 0, 0, 0, 0, 'a', encoded, false)
        then:
        vRemoved.isRemove()
        !vRemoved.isExpired()

        when:
        def vExpired = new Wal.V(1, 0, 0, System.currentTimeMillis() - 1000, 0, 'a', 'a'.bytes, true)
        then:
        vExpired.isExpired()
        !vExpired.isRemove()

        when:
        def vNotExpired = new Wal.V(1, 0, 0, System.currentTimeMillis() + 1000, 0, 'a', 'a'.bytes, true)
        then:
        !vNotExpired.isExpired()
    }

    def 'put and get'() {
        given:
        ConfForGlobal.pureMemory = false
        ConfForSlot.global = ConfForSlot.debugMode

        def file = new File(Consts.slotDir, 'test-raf.wal')
        def fileShortValue = new File(Consts.slotDir, 'test-raf-short-value.wal')
        if (file.exists()) {
            file.delete()
        }
        if (fileShortValue.exists()) {
            fileShortValue.delete()
        }

        FileUtils.touch(file)
        FileUtils.touch(fileShortValue)

        println file.absolutePath
        println fileShortValue.absolutePath

        def v1 = Mock.prepareValueList(1)[0]
        println 'Mock Wal.V, v1: ' + v1 + ', persist length: ' + v1.persistLength()
        println Wal.V.persistLength(v1.key().length(), v1.cvEncoded().length)

        def raf = new RandomAccessFile(file, 'rw')
        def rafShortValue = new RandomAccessFile(fileShortValue, 'rw')
        def snowFlake = new SnowFlake(1, 1)
        def oneSlot = new OneSlot(slot)
        def wal = new Wal(slot, oneSlot, 0, raf, rafShortValue, snowFlake)
        def wal2 = new Wal(slot, oneSlot, 1, raf, rafShortValue, snowFlake)
        println 'Wal: ' + wal
        println 'Wal2: ' + wal2
        println 'in memory size estimate: ' + wal.estimate(new StringBuilder())

        expect:
        Wal.calcWalGroupIndex(0) == 0
        Wal.calcWalGroupIndex(ConfForSlot.global.confWal.oneChargeBucketNumber) == 1
        Wal.calcWalGroupNumber() == 4096 / 32
        wal.lastSeqAfterPut == 0
        wal.lastSeqShortValueAfterPut == 0
        wal.writePosition == 0
        wal.writePositionShortValue == 0

        when:
        wal.lazyReadFromFile()
        then:
        1 == 1

        when:
        ConfForGlobal.pureMemory = true
        wal.lazyReadFromFile()
        then:
        1 == 1

        when:
        ConfForGlobal.pureMemory = false
        def vList = Mock.prepareValueList(10, 0) { v ->
            if (v.seq() == 9) {
                def cv = CompressedValue.decode(Unpooled.wrappedBuffer(v.cvEncoded()), v.key().bytes, v.keyHash())
                cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
                cv.setCompressedDataAsBigString(1234L, CompressedValue.NULL_DICT_SEQ)
                def bigStringEncoded = cv.encode()
                def v2 = new Wal.V(v.seq(), v.bucketIndex(), v.keyHash(), CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                        v.key(), bigStringEncoded, false)
                return v2
            }

            return v
        }
        vList.each { v ->
            def key = v.key()
            wal.put(true, key, v)

            def bytes = wal.get(key)
            def cv2 = CompressedValue.decode(Unpooled.wrappedBuffer(bytes), key.bytes, v.keyHash())
            def value2 = new String(cv2.compressedData)
            println "key: $key, cv2: $cv2, value2: $value2"
        }
        println 'in memory size estimate: ' + wal.estimate(new StringBuilder())
        HashMap<String, Wal.V> toMap = [:]
        HashMap<String, Wal.V> toMap2 = [:]
        wal.readWal(rafShortValue, toMap, true)
        wal.readWal(rafShortValue, toMap2, false)
        println toMap.keySet().join(',')
        then:
        toMap.size() == 10
        wal.keyCount == 10
        wal.lastSeqShortValueAfterPut == vList[-1].seq()
        wal.bigStringFileUuidByKey.size() == 1
        wal.bigStringFileUuidByKey.containsValue 1234L

        when:
        def vBytes = new byte[2]
        def vDecoded = Wal.V.decode(new DataInputStream(new ByteArrayInputStream(vBytes)))
        then:
        vDecoded == null

        when:
        boolean exception = false
        def v1Encoded = v1.encode(false)
        def v1Encoded_end0 = v1.encode(true)
        def v1Buffer = ByteBuffer.wrap(v1Encoded)
        v1Buffer.putShort(36, (CompressedValue.KEY_MAX_LENGTH + 1).shortValue())
        try {
            Wal.V.decode(new DataInputStream(new ByteArrayInputStream(v1Encoded)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        v1Buffer.putShort(36, (short) -1)
        try {
            Wal.V.decode(new DataInputStream(new ByteArrayInputStream(v1Encoded)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        v1Buffer.putShort(36, (short) v1.key().length())
        v1Buffer.putInt(0, 1)
        try {
            Wal.V.decode(new DataInputStream(new ByteArrayInputStream(v1Encoded)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def n = wal.readWal(null, toMap, true)
        then:
        n == 0

        when:
        def n1 = wal.readFromSavedBytes(new byte[4], true)
        def n11 = wal.readFromSavedBytes(new byte[4], false)
        then:
        n1 == 0
        n11 == 0

        when:
        def bytes1 = wal.writeToSavedBytes(true, false)
        def bytes11 = wal.writeToSavedBytes(false, true)
        n1 = wal.readFromSavedBytes(bytes1, true)
        n11 = wal.readFromSavedBytes(bytes11, false)
        then:
        n1 == 10
        n11 == 0

        // repl
        // repl export exists batch to slave
        when:
        ConfForGlobal.pureMemory = false
        wal.put(false, 'xyz', vList[-1])
        def toSlaveExistsBytes1 = wal.toSlaveExistsOneWalGroupBytes()
        then:
        toSlaveExistsBytes1.length == 32 + Wal.ONE_GROUP_BUFFER_SIZE * 2

        when:
        ConfForGlobal.pureMemory = true
        def toSlaveExistsBytes2 = wal.toSlaveExistsOneWalGroupBytes()
        then:
        toSlaveExistsBytes2.length == toSlaveExistsBytes1.length

        // repl import exists batch from master
        when:
        ConfForGlobal.pureMemory = false
        def wal11 = new Wal(slot, oneSlot, 0, raf, rafShortValue, snowFlake)
        def wal22 = new Wal(slot, oneSlot, 0, raf, rafShortValue, snowFlake)
        wal11.fromMasterExistsOneWalGroupBytes(toSlaveExistsBytes1)
        wal22.fromMasterExistsOneWalGroupBytes(toSlaveExistsBytes2)
        then:
        wal.delayToKeyBucketValues.size() == wal11.delayToKeyBucketValues.size()
        wal.delayToKeyBucketShortValues.size() == wal11.delayToKeyBucketShortValues.size()
        wal11.delayToKeyBucketValues == wal22.delayToKeyBucketValues
        wal11.delayToKeyBucketShortValues == wal22.delayToKeyBucketShortValues

        when:
        def oldOneGroupBufferSize = Wal.ONE_GROUP_BUFFER_SIZE
        Wal.ONE_GROUP_BUFFER_SIZE = oldOneGroupBufferSize * 2
        exception = false
        // buffer size not match
        try {
            wal11.fromMasterExistsOneWalGroupBytes(toSlaveExistsBytes1)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        Wal.ONE_GROUP_BUFFER_SIZE = oldOneGroupBufferSize
        exception = false
        try {
            wal2.fromMasterExistsOneWalGroupBytes(toSlaveExistsBytes1)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        // rewrite
        when:
        def shortV = Mock.prepareValueList(1)[0]
        List<Wal.PutResult> resultList = []
        1000.times {
            resultList << wal.put(true, 'short00', shortV)
            resultList << wal.put(false, 'normal00', shortV)
        }
        then:
        resultList.every { !it.needPersist() }

        // put from x (slave replay / apply)
        when:
        wal.putFromX(v1, true, wal.writePositionShortValue)
        wal.putFromX(v1, false, wal.writePosition)
        then:
        1 == 1

        when:
        ConfForGlobal.pureMemory = true
        wal.putFromX(v1, true, wal.writePositionShortValue)
        wal.putFromX(v1, false, wal.writePosition)
        then:
        1 == 1

        when:
        ConfForGlobal.pureMemory = false
        wal.clearValues()
        wal.clearShortValues()
        then:
        wal.delayToKeyBucketValues.size() == 0
        wal.delayToKeyBucketShortValues.size() == 0
        wal.lastSeqAfterPut == 0
        wal.lastSeqShortValueAfterPut == 0

        cleanup:
        wal.clear()
        wal.clear(false)
        wal2.clear()
        wal2.clear(false)
        raf.close()
        rafShortValue.close()
        file.delete()
        fileShortValue.delete()
    }

    def 'test value change to short value'() {
        given:
        ConfForGlobal.pureMemory = true

        def snowFlake = new SnowFlake(1, 1)
        def wal = new Wal(slot, null, 0, null, null, snowFlake)

        def key = 'test-key'
        def shortV = new Wal.V(1, 0, 0, 0, 0, key, 'short-value'.bytes, false)
        def v = new Wal.V(2, 0, 0, 0, 0, key, 'value'.bytes, false)
        def shortV2 = new Wal.V(3, 0, 0, 0, 0, key, 'short-value-x'.bytes, false)

        expect:
        wal.get(key) == null

        when:
        wal.delayToKeyBucketShortValues.put(key, shortV)
        then:
        wal.get(key) == 'short-value'.bytes

        when:
        wal.delayToKeyBucketValues.put(key, v)
        then:
        wal.get(key) == 'value'.bytes

        when:
        wal.delayToKeyBucketShortValues.put(key, shortV2)
        then:
        wal.get(key) == 'short-value-x'.bytes

        when:
        wal.delayToKeyBucketShortValues.remove(key)
        then:
        wal.get(key) == 'value'.bytes

        when:
        wal.delayToKeyBucketValues.remove(key)
        wal.delayToKeyBucketShortValues.remove(key)
        then:
        wal.get(key) == null

        when:
        wal.removeDelay(key, 0, v.keyHash(), 0L)
        def cvEncoded = wal.get(key)
        then:
        cvEncoded != null && cvEncoded.length == 1
        !wal.exists(key)

        when:
        wal.put(true, key, v)
        then:
        wal.exists(key)
        wal.hasKey(key)
        !wal.hasKey(key + '-not-exist')

        when:
        wal.put(false, key, v)
        then:
        wal.exists(key)
        !wal.exists(key + '-not-exist')
        wal.hasKey(key)

        when:
        def longV = new Wal.V(4, 0, 0, 0, 0, key, ('long-value' * 100).bytes, false)
        def longKey = 'long-key'
        List<Wal.PutResult> putResultList = []
        100.times {
            putResultList << wal.put(false, longKey + it, longV)
        }
        then:
        putResultList.size() == 100

        when:
        wal.clearValues()
        putResultList.clear()
        100.times {
            putResultList << wal.put(true, longKey + it, longV)
        }
        then:
        putResultList.size() == 100

        when:
        wal.clearShortValues()
        putResultList.clear()
        ConfForSlot.global.confWal.valueSizeTrigger = 100
        Wal.ONE_GROUP_BUFFER_SIZE = 256 * 1024
        100.times {
            putResultList << wal.put(false, longKey + it, longV)
        }
        then:
        putResultList.size() == 100

        when:
        wal.clearValues()
        wal.clearShortValues()
        putResultList.clear()
        ConfForSlot.global.confWal.shortValueSizeTrigger = 100
        100.times {
            putResultList << wal.put(true, longKey + it, longV)
        }
        then:
        putResultList.size() == 100

        when:
        wal.clearShortValuesCount = 999
        wal.clearValuesCount = 999
        wal.clearShortValues()
        wal.clearValues()
        then:
        wal.get(key) == null
        wal.keyCount == 0

        cleanup:
        ConfForGlobal.pureMemory = false
        Wal.ONE_GROUP_BUFFER_SIZE = 64 * 1024
    }

    def 'test scan'() {
        given:
        ConfForGlobal.pureMemory = true

        def snowFlake = new SnowFlake(1, 1)
        def wal = new Wal(slot, null, 0, null, null, snowFlake)

        expect:
        wal.inWalKeysFormScan(0L).isEmpty()

        when:
        def r = wal.scan((short) 0, KeyLoader.typeAsByteIgnore, null, 10, 100L)
        then:
        r.keys().isEmpty()

        when:
        def shortValueList = Mock.prepareShortValueList(10, 0)
        for (shortV in shortValueList) {
            wal.put(true, shortV.key(), shortV)
        }
        r = wal.scan((short) 0, KeyLoader.typeAsByteIgnore, null, 10, 100L)
        then:
        r != null
        r.keys().size() == 10
        wal.inWalKeysFormScan(100L).size() == 10

        when:
        r = wal.scan((short) 5, KeyLoader.typeAsByteIgnore, null, 10, 100L)
        then:
        r != null
        r.keys().size() == 5
        // go to next wal group
        r.scanCursor().walGroupIndex() == 1

        when:
        // all wal v seq is > 0
        r = wal.scan((short) 5, KeyLoader.typeAsByteIgnore, null, 10, 0L)
        then:
        r.keys().isEmpty()

        when:
        // type is not matched
        r = wal.scan((short) 0, KeyLoader.typeAsByteHash, null, 10, 100L)
        then:
        r.keys().isEmpty()

        when:
        // prefix is not matched
        r = wal.scan((short) 0, KeyLoader.typeAsByteIgnore, 'xxx:', 10, 100L)
        then:
        r.keys().isEmpty()

        when:
        wal.clear()
        def shortValueList2 = Mock.prepareShortValueList(10, 0) { v ->
            // mock some removed
            if (v.seq() == 9) {
                byte[] encoded = [CompressedValue.SP_FLAG_DELETE_TMP]
                return new Wal.V(v.seq(), 0, v.keyHash(), 0L, 0,
                        v.key(), encoded, false)
            }

            // mock some expired
            def v2 = new Wal.V(v.seq(), 0, v.keyHash(), v.seq() % 2 == 0 ? System.currentTimeMillis() - 1000 : 0L, 0,
                    v.key(), v.cvEncoded(), false)
            return v2
        }
        for (shortV in shortValueList2) {
            wal.put(true, shortV.key(), shortV)
        }
        r = wal.scan((short) 5, KeyLoader.typeAsByteIgnore, null, 1, 100L)
        then:
        // 1 removed and 5 expired
        r.keys().size() == 1
        r.scanCursor().walSkipCount() == 7

        when:
        def lastWalGroup = Wal.calcWalGroupNumber() - 1
        def lastWal = new Wal(slot, null, lastWalGroup, null, null, snowFlake)
        r = lastWal.scan((short) 0, KeyLoader.typeAsByteIgnore, null, 10, 100L)
        then:
        r.keys().isEmpty()

        when:
        def valueList = Mock.prepareValueList(10, 0)
        for (v in valueList) {
            wal.put(false, v.key(), v)
        }
        then:
        !wal.inWalKeysFormScan(5L).isEmpty()

        cleanup:
        ConfForGlobal.pureMemory = false
    }

    def 'test reset write position'() {
        given:
        Debug.instance.bulkLoad = true

        and:
        def file = new File(Consts.slotDir, 'test-raf.wal')
        def fileShortValue = new File(Consts.slotDir, 'test-raf-short-value.wal')
        if (file.exists()) {
            file.delete()
        }
        if (fileShortValue.exists()) {
            fileShortValue.delete()
        }

        FileUtils.touch(file)
        FileUtils.touch(fileShortValue)

        def raf = new RandomAccessFile(file, 'rw')
        def rafShortValue = new RandomAccessFile(fileShortValue, 'rw')
        def snowFlake = new SnowFlake(1, 1)
        def oneSlot = new OneSlot(slot)
        def wal = new Wal(slot, oneSlot, 0, raf, rafShortValue, snowFlake)

        when:
        def vList = Mock.prepareValueList(10)
        wal.put(false, vList[0].key(), vList[0])
        wal.put(false, vList[1].key(), vList[1])
        wal.put(true, vList[2].key(), vList[2])
        wal.put(true, vList[3].key(), vList[3])
        def writePosition = wal.writePosition
        def writePositionShortValue = wal.writePositionShortValue
        wal.resetWritePositionAfterBulkLoad()
        then:
        writePosition == wal.writePosition
        writePositionShortValue == wal.writePositionShortValue

        when:
        // reload
        def walReloaded = new Wal(slot, oneSlot, 0, raf, rafShortValue, snowFlake)
        walReloaded.lazyReadFromFile()
        then:
        walReloaded.delayToKeyBucketValues.size() == 2
        walReloaded.delayToKeyBucketShortValues.size() == 2
        walReloaded.get(vList[0].key()) == vList[0].cvEncoded()
        walReloaded.get(vList[2].key()) == vList[2].cvEncoded()

        cleanup:
        raf.close()
        rafShortValue.close()
        file.delete()
        fileShortValue.delete()
        Debug.instance.bulkLoad = false
    }

    def 'test delete expired big string files'() {
        given:
        ConfForGlobal.pureMemory = false
        ConfForSlot.global = ConfForSlot.debugMode

        def file = new File(Consts.slotDir, 'test-raf.wal')
        def fileShortValue = new File(Consts.slotDir, 'test-raf-short-value.wal')
        if (file.exists()) {
            file.delete()
        }
        if (fileShortValue.exists()) {
            fileShortValue.delete()
        }

        def raf = new RandomAccessFile(file, 'rw')
        def rafShortValue = new RandomAccessFile(fileShortValue, 'rw')
        def snowFlake = new SnowFlake(1, 1)
        def oneSlot = new OneSlot(slot)
        def wal = new Wal(slot, oneSlot, 0, raf, rafShortValue, snowFlake)

        when:
        def count = wal.intervalDeleteExpiredBigStringFiles()
        then:
        count == 0

        when:
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.keyHash = KeyHash.hash('a'.bytes)
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv.setCompressedDataAsBigString(1234L, Dict.SELF_ZSTD_DICT_SEQ)
        def v = new Wal.V(1, 0, cv.keyHash, System.currentTimeMillis() - 1000, 0, 'a', cv.encode(), false)
        wal.put(true, 'xxxx', v)
        count = wal.intervalDeleteExpiredBigStringFiles()
        then:
        count == 1

        cleanup:
        wal.clear()
        wal.clear(false)
    }
}
