package io.velo.persist

import io.netty.buffer.Unpooled
import io.velo.CompressedValue
import io.velo.ConfForGlobal
import io.velo.ConfForSlot
import io.velo.SnowFlake
import org.apache.commons.io.FileUtils
import spock.lang.Specification

import java.nio.ByteBuffer

class WalTest extends Specification {
    final short slot = 0

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

        and:
        ConfForSlot.global.confWal.resetWalStaticValues(Wal.ONE_GROUP_BUFFER_SIZE)

        def raf = new RandomAccessFile(file, 'rw')
        def rafShortValue = new RandomAccessFile(fileShortValue, 'rw')
        def snowFlake = new SnowFlake(1, 1)
        def wal = new Wal(slot, 0, raf, rafShortValue, snowFlake)
        def wal2 = new Wal(slot, 1, raf, rafShortValue, snowFlake)
        println 'Wal: ' + wal
        println 'Wal2: ' + wal2
        println 'in memory size estimate: ' + wal.estimate()

        expect:
        Wal.calWalGroupIndex(0) == 0
        Wal.calWalGroupIndex(ConfForSlot.global.confWal.oneChargeBucketNumber) == 1
        Wal.calcWalGroupNumber() == 4096 / 32
        wal.lastSeqAfterPut == 0
        wal.lastSeqShortValueAfterPut == 0

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
        def vList = Mock.prepareValueList(10)
        vList.each { v ->
            def key = v.key()
            wal.put(true, key, v)

            def bytes = wal.get(key)
            def cv2 = CompressedValue.decode(Unpooled.wrappedBuffer(bytes), key.bytes, v.keyHash())
            def value2 = new String(cv2.compressedData)
            println "key: $key, cv2: $cv2, value2: $value2"
        }
        println 'in memory size estimate: ' + wal.estimate()
        HashMap<String, Wal.V> toMap = [:]
        HashMap<String, Wal.V> toMap2 = [:]
        wal.readWal(rafShortValue, toMap, true)
        wal.readWal(rafShortValue, toMap2, false)
        println toMap.keySet().join(',')
        then:
        toMap.size() == 10
        wal.keyCount == 10
        wal.lastSeqShortValueAfterPut == vList[-1].seq()

        when:
        def vBytes = new byte[2]
        def vDecoded = Wal.V.decode(new DataInputStream(new ByteArrayInputStream(vBytes)))
        then:
        vDecoded == null

        when:
        boolean exception = false
        def v1Encoded = v1.encode()
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

        // repl
        // repl export exists batch to slave
        when:
        ConfForGlobal.pureMemory = false
        def toSlaveExistsBytes = wal.toSlaveExistsOneWalGroupBytes()
        then:
        toSlaveExistsBytes.length == 32 + Wal.ONE_GROUP_BUFFER_SIZE * 2

        // repl import exists batch from master
        when:
        def wal11 = new Wal(slot, 0, raf, rafShortValue, snowFlake)
        wal11.fromMasterExistsOneWalGroupBytes(toSlaveExistsBytes)
        then:
        wal.delayToKeyBucketValues.size() == wal11.delayToKeyBucketValues.size()
        wal.delayToKeyBucketShortValues.size() == wal11.delayToKeyBucketShortValues.size()

        when:
        def oldOneGroupBufferSize = Wal.ONE_GROUP_BUFFER_SIZE
        Wal.ONE_GROUP_BUFFER_SIZE = oldOneGroupBufferSize * 2
        exception = false
        // buffer size not match
        try {
            wal11.fromMasterExistsOneWalGroupBytes(toSlaveExistsBytes)
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
            wal2.fromMasterExistsOneWalGroupBytes(toSlaveExistsBytes)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

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
        def wal = new Wal(slot, 0, null, null, snowFlake)

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
        wal.removeDelay(key, 0, v.keyHash())
        def cvEncoded = wal.get(key)
        then:
        cvEncoded != null && cvEncoded.length == 1
        !wal.exists(key)

        when:
        wal.put(true, key, v)
        then:
        wal.exists(key)

        when:
        wal.put(false, key, v)
        then:
        wal.exists(key)
        !wal.exists(key + '-not-exist')

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
}
