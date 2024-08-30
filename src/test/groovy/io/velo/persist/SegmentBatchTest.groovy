package io.velo.persist

import com.github.luben.zstd.Zstd
import io.velo.CompressedValue
import io.velo.SnowFlake
import spock.lang.Specification

import java.nio.ByteBuffer

class SegmentBatchTest extends Specification {
    final short slot = 0

    def 'tight segments write and read'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch = new SegmentBatch(slot, snowFlake)

        segmentBatch.collect()
        segmentBatch.compressCountTotal = 1
        segmentBatch.compressBytesTotal = 1
        segmentBatch.batchCountTotal = 1
        segmentBatch.afterTightSegmentCountTotal = 1
        segmentBatch.collect()

        println new SegmentBatch.SegmentCompressedBytesWithIndex(new byte[10], 0, 10L)
        new SegmentBatch.ForDebugCvCallback().callback('a', new CompressedValue(), 0)

        and:
        def list = Mock.prepareValueList(800)

        int[] nextNSegmentIndex = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]
        int[] nextNSegmentIndex2 = [0, 1, 2, 3, 4, 5, 6]
        ArrayList<PersistValueMeta> returnPvmList = []
        ArrayList<PersistValueMeta> returnPvmList2 = []

        expect:
        SegmentBatch.subBlockMetaPosition(0) == 12
        SegmentBatch.subBlockMetaPosition(1) == 16
        SegmentBatch.subBlockMetaPosition(2) == 20
        SegmentBatch.subBlockMetaPosition(3) == 24

        when:
        boolean exception = false
        try {
            segmentBatch.splitAndTight(list, nextNSegmentIndex2, returnPvmList2)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        nextNSegmentIndex2 = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]
        exception = false
        try {
            segmentBatch.splitAndTight(list, nextNSegmentIndex2, returnPvmList2)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def r = segmentBatch.splitAndTight(list, nextNSegmentIndex, returnPvmList)
        for (one in r) {
            println one
        }
        def first = r[0]
        def buffer = ByteBuffer.wrap(first.tightBytesWithLength())
        def seq = buffer.getLong()
        def totalBytesN = buffer.getInt()
        println "seq: $seq, total bytes: $totalBytesN"

        List<CompressedValue> loaded = []
        for (i in 0..<SegmentBatch.MAX_BLOCK_NUMBER) {
            buffer.position(8 + 4 + i * (2 + 2))
            def offset = buffer.getShort()
            def length = buffer.getShort()

            if (offset == 0) {
                assert length == 0
                break
            }

            def compressedBytes = new byte[length]
            buffer.position(offset).get(compressedBytes)

            println "offset: $offset, compressed bytes length: ${compressedBytes.length}"

            def decompressedBytes = Zstd.decompress(compressedBytes, 4096)
            println "decompressed bytes length: ${decompressedBytes.length}"

            SegmentBatch.iterateFromSegmentBytes(decompressedBytes, { key, cv, offsetInThisSegment ->
                if (cv.seq % 10 == 0) {
                    println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
                }
                loaded << cv
            })
        }

        then:
        returnPvmList.size() == list.size()
        loaded.every { one ->
            one.compressedLength == 10 &&
                    list.find { it.seq() == one.seq }.keyHash() == one.keyHash
        }

        when:
        def bytesX = new byte[16]
        List<CompressedValue> loaded2 = []
        segmentBatch.iterateFromSegmentBytes(bytesX, { key, cv, offsetInThisSegment ->
            println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
            loaded2 << cv
        })
        then:
        loaded2.size() == 0

        when:
        bytesX = new byte[18]
        segmentBatch.iterateFromSegmentBytes(bytesX, { key, cv, offsetInThisSegment ->
            println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
            loaded2 << cv
        })
        then:
        loaded2.size() == 0

        when:
        exception = false
        ByteBuffer.wrap(bytesX).putShort(16, (short) -1)
        try {
            segmentBatch.iterateFromSegmentBytes(bytesX, { key, cv, offsetInThisSegment ->
                println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
                loaded2 << cv
            })
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
