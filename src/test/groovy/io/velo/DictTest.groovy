package io.velo

import com.github.luben.zstd.Zstd
import io.velo.persist.Consts
import spock.lang.Specification

import java.nio.ByteBuffer

class DictTest extends Specification {
    def 'test all'() {
        given:
        def dict = new Dict()
        println dict
        println dict.hashCode()

        dict.seq = Dict.SELF_ZSTD_DICT_SEQ
        dict.dictBytes = new byte[10]

        def dict2 = new Dict()
        dict2.seq = 0
        dict2.createdTime = System.currentTimeMillis()
        println dict2.createdTime

        expect:
        Dict.SELF_ZSTD_DICT.seq == Dict.SELF_ZSTD_DICT_SEQ
        dict.equals(dict)
        dict == Dict.SELF_ZSTD_DICT
        !dict.equals(null)
        dict != new String('xxx')
        dict != dict2

        when:
        HashSet<Integer> seqSet = []
        10000.times {
            seqSet << Dict.generateRandomSeq()
        }
        then:
        // random may conflict
        seqSet.size() == 10000 || seqSet.size() == 9999
    }

    def 'test generate random seq never collides with self zstd dict seq'() {
        given: 'a random source that always returns 0 — the worst case for the old formula'
        def allZerosRandom = new Random() {
            @Override
            int nextInt(int bound) {
                return 0
            }
        }
        Dict.testOnlyRandom = allZerosRandom

        expect:
        // The fix guarantees the minimum return value is 1_000_001.
        // Asserting > (not !=) also catches a regression that produces a negative
        // seq from integer overflow, which != SELF_ZSTD_DICT_SEQ would miss.
        Dict.generateRandomSeq() > Dict.SELF_ZSTD_DICT_SEQ

        cleanup:
        Dict.testOnlyRandom = null
    }

    def 'test decode'() {
        given:
        def dict = new Dict(new byte[10])
        def keyPrefix = 'test'

        def encoded = dict.encode(keyPrefix)

        def dictWithKeyPrefixOrSuffix = Dict.decode(new DataInputStream(new ByteArrayInputStream(encoded)))
        println dictWithKeyPrefixOrSuffix.toString()
        def dict2 = dictWithKeyPrefixOrSuffix.dict()

        expect:
        dict.encodeLength(keyPrefix) == encoded.length
        dictWithKeyPrefixOrSuffix.keyPrefixOrSuffix() == keyPrefix
        dict == dict2
        dict2.dictBytes == dict.dictBytes

        when:
        def dict3 = Dict.decode(new DataInputStream(new ByteArrayInputStream(new byte[3])))
        then:
        dict3 == null

        when:
        def dict4 = Dict.decode(new DataInputStream(new ByteArrayInputStream(new byte[4])))
        then:
        dict4 == null

        when:
        byte[] testEncodedBytes = new byte[encoded.length]
        def buffer = ByteBuffer.wrap(testEncodedBytes)
        buffer.putInt(0, encoded.length - 4)
        buffer.putShort(4 + 4 + 8, (short) (CompressedValue.KEY_MAX_LENGTH + 1))
        boolean exception = false
        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(testEncodedBytes)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        buffer.putShort(4 + 4 + 8, (short) 0)
        exception = false
        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(testEncodedBytes)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        buffer.putInt(0, encoded.length - 4 - 1)
        buffer.putShort(4 + 4 + 8, (short) keyPrefix.length())
        exception = false
        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(testEncodedBytes)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when: 'corrupt dict bytes length is negative'
        byte[] corruptNegBytes = new byte[4 + 4 + 8 + 2 + keyPrefix.length() + 2]
        def corruptNegBuffer = ByteBuffer.wrap(corruptNegBytes)
        corruptNegBuffer.putInt(0, corruptNegBytes.length - 4)
        corruptNegBuffer.putInt(4, dict.seq)
        corruptNegBuffer.putLong(8, dict.createdTime)
        corruptNegBuffer.putShort(16, (short) keyPrefix.length())
        corruptNegBuffer.position(18)
        corruptNegBuffer.put(keyPrefix.bytes)
        corruptNegBuffer.putShort(18 + keyPrefix.length(), (short) -1)
        exception = false
        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(corruptNegBytes)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            new Dict(new byte[Short.MAX_VALUE + 1])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test encode and decode round trip with non ascii prefix'() {
        given: 'a dict and a non-ascii prefix (utf-8 bytes > char count)'
        def dict = new Dict(new byte[8])
        def keyPrefix = '用户'  // 2 chars, 6 UTF-8 bytes (3 bytes per char)

        when: 'encoded'
        def encoded = dict.encode(keyPrefix)

        then: 'no exception, encodeLength matches actual byte length, and decode round-trips'
        // Old bug: char count (2) was used to size the buffer but getBytes() wrote 6 bytes,
        // causing BufferOverflowException. Fix must use UTF-8 byte count consistently.
        dict.encodeLength(keyPrefix) == encoded.length

        when: 'decoded back'
        def decoded = Dict.decode(new DataInputStream(new ByteArrayInputStream(encoded)))

        then: 'the original string is recovered'
        decoded != null
        decoded.keyPrefixOrSuffix() == keyPrefix
        decoded.dict().dictBytes == dict.dictBytes
    }

    def 'test init ctx'() {
        given:
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.testDir)

        and:
        def job = new TrainSampleJob((byte) 0)
        job.dictSize = 512
        job.trainSampleMinBodyLength = 1024

        and:
        def sampleValue = 'xxxx' * 5 + 'yyyy' * 5 + 'zzzz' * 5
        def sampleValueBytes = sampleValue.bytes

        def snowFlake = new SnowFlake(0, 0)


        def keyPrefix = 'key:'
        TrainSampleJob.keyPrefixOrSuffixGroupList = [keyPrefix]
        List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
        11.times {
            sampleToTrainList << new TrainSampleJob.TrainSampleKV("key:$it", null, snowFlake.nextId(), sampleValueBytes)
        }

        job.resetSampleToTrainList(sampleToTrainList)
        def result = job.train()

        expect:
        result.cacheDict().size() == 1

        when:
        def dictTrained = result.cacheDict().get(keyPrefix)
        dictMap.putDict(keyPrefix, dictTrained)
        then:
        dictTrained.decompressCtxArray != null
        dictTrained.ctxCompressArray != null

        when:
        def sampleCompressedValueBytes = dictTrained.compressByteArray(sampleValueBytes)
        println 'compressed length: ' + sampleCompressedValueBytes.length
        println 'uncompressed length: ' + sampleValueBytes.length
        println 'compress ratio: ' + (sampleCompressedValueBytes.length / sampleValueBytes.length)
        then:
        sampleCompressedValueBytes.length < sampleValueBytes.length

        when:
        def dst = new byte[((int) Zstd.compressBound(sampleValueBytes.length))]
        def compressedSize = dictTrained.compressByteArray(dst, 0, sampleValueBytes, 0, sampleValueBytes.length)
        then:
        compressedSize == sampleCompressedValueBytes.length

        when:
        def dst2 = new byte[sampleValueBytes.length]
        dictTrained.decompressByteArray(dst2, 0, sampleCompressedValueBytes, 0, sampleCompressedValueBytes.length)
        then:
        dst2 == sampleValueBytes

        // loop compress and single record decompress
        when:
        def srcBuffer = ByteBuffer.allocateDirect(1024)
        def src2Buffer = ByteBuffer.allocateDirect(1024)
        def dstBuffer = ByteBuffer.allocateDirect(1024 * 1024)
        def boundDstSize = (int) Zstd.compressBound(sampleValueBytes.length)
        10.times {
            srcBuffer.put(sampleValueBytes)
        }
        int dstOffset = 0
        long now = System.nanoTime()
        for (i in 0..<10) {
            def xx = dictTrained.compressByteBuffer(dstBuffer, dstOffset, boundDstSize, srcBuffer, 0, sampleValueBytes.length)
            println 'dst offset: ' + dstOffset + ', compressed length: ' + xx
            dstOffset += xx
        }
        println 'compress byte buffer 10 times cost: ' + (System.nanoTime() - now) / 1000 + 'us'
        then:
        dictTrained.decompressByteBuffer(src2Buffer, 0, sampleValueBytes.length, dstBuffer, 0, sampleCompressedValueBytes.length) == sampleValueBytes.length

        when:
        def afterDecompressedBytes = new byte[sampleValueBytes.length]
        src2Buffer.position(0).get(afterDecompressedBytes)
        then:
        afterDecompressedBytes == sampleValueBytes

        when:
        srcBuffer.position(0)
        def dstBuffer2 = ByteBuffer.allocateDirect(1024 * 1024)
        def now2 = System.nanoTime()
        def xx2 = dictTrained.compressByteBuffer(dstBuffer2, 0, boundDstSize, srcBuffer, 0, sampleValueBytes.length * 10)
        println 'compress byte buffer 1 time cost: ' + (System.nanoTime() - now2) / 1000 + 'us'
        def sampleValueBytes10 = new byte[sampleValueBytes.length * 10]
        def sampleValueBytesBuffer = ByteBuffer.wrap(sampleValueBytes10)
        10.times {
            sampleValueBytesBuffer.put(sampleValueBytes)
        }
        then:
        xx2 == Zstd.compressUsingDict(sampleValueBytes10, dictTrained.dictBytes, 3).length

        cleanup:
        // for coverage
        dictTrained.initCtx()
        dictTrained.closeCtx()
        dictMap.cleanUp()
    }

    def 'test metrics'() {
        given:
        def dict = new Dict()

        expect:
        dict.compressedRatio() == 0

        when:
        dict.compressedCountTotal.add(1)
        dict.compressedBytesTotal.add(10)
        dict.compressBytesTotal.add(100)
        then:
        dict.compressedRatio() == 0.1
    }
}
