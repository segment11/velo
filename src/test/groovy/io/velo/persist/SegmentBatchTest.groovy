package io.velo.persist

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
        new SegmentBatch2.ForDebugCvCallback().callback('a', new CompressedValue(), 0)

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
        def r2 = segmentBatch.split(list, nextNSegmentIndex, [])
        then:
        r.size() == r2.size()
        returnPvmList.size() == list.size()

        when:
        def first = r[0]
        def buffer = ByteBuffer.wrap(first.tightBytesWithLength())
        def seq = buffer.getLong()
        def totalBytesN = buffer.getInt()
        println "seq: $seq, total bytes: $totalBytesN"
        List<CompressedValue> loadedCvList = []
        def pvm0 = new PersistValueMeta()
        pvm0.slot = slot
        pvm0.segmentIndex = 0
        def mockChunk = ChunkTest.prepareOne(slot)
        for (i in 0..<SegmentBatch.MAX_BLOCK_NUMBER) {
            pvm0.subBlockIndex = (byte) i
            def decompressedSegmentBytes0 = SegmentBatch.decompressSegmentBytesFromOneSubBlock(first.tightBytesWithLength(), pvm0, mockChunk)
            SegmentBatch2.iterateFromSegmentBytes(decompressedSegmentBytes0, 0, decompressedSegmentBytes0.length, { key, cv, offsetInThisSegment ->
                if (cv.seq % 10 == 0) {
                    println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
                }
                loadedCvList << cv
            })
        }
        then:
        loadedCvList.every { one ->
            one.compressedLength == 10 &&
                    list.find { it.seq() == one.seq }.keyHash() == one.keyHash
        }
    }
}
