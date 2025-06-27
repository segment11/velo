package io.velo.persist

import io.velo.CompressedValue
import io.velo.SnowFlake
import spock.lang.Specification

import java.nio.ByteBuffer

class SegmentBatch2Test extends Specification {
    final short slot = 0

    def 'split segments write and read'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch2 = new SegmentBatch2(slot, snowFlake)

        segmentBatch2.collect()
        segmentBatch2.batchCountTotal = 1
        segmentBatch2.collect()

        println new SegmentBatch2.SegmentBytesWithIndex(new byte[10], 0, 10L, 0)
        new SegmentBatch2.ForDebugCvCallback().callback('a', new CompressedValue(), 0)

        and:
        def list = Mock.prepareValueList(800)
        ArrayList<PersistValueMeta> returnPvmList = []

        when:
        def r = segmentBatch2.split(list, returnPvmList)
        for (one in r) {
            println one
        }
        def first = r[0]
        def segmentBytes = first.segmentBytes()
        List<CompressedValue> loaded = []
        SegmentBatch2.iterateFromSegmentBytes(segmentBytes, { key, cv, offsetInThisSegment ->
            if (cv.seq % 10 == 0) {
                println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
            }
            loaded << cv
        })
        then:
        returnPvmList.size() == list.size()
        loaded.every { one ->
            one.compressedLength == 10 &&
                    list.find { it.seq() == one.seq }.keyHash() == one.keyHash
        }

        when:
        def bytesX = new byte[SegmentBatch2.SEGMENT_HEADER_LENGTH]
        List<CompressedValue> loaded2 = []
        SegmentBatch2.iterateFromSegmentBytes(bytesX, { key, cv, offsetInThisSegment ->
            println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
            loaded2 << cv
        })
        then:
        loaded2.size() == 0

        when:
        bytesX = new byte[SegmentBatch2.SEGMENT_HEADER_LENGTH + 2]
        SegmentBatch2.iterateFromSegmentBytes(bytesX, { key, cv, offsetInThisSegment ->
            println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
            loaded2 << cv
        })
        then:
        loaded2.size() == 0

        when:
        boolean exception = false
        ByteBuffer.wrap(bytesX).putShort(SegmentBatch2.SEGMENT_HEADER_LENGTH, (short) -1)
        try {
            SegmentBatch2.iterateFromSegmentBytes(bytesX, 0, bytesX.length, { key, cv, offsetInThisSegment ->
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

    def 'test segment slim'() {
        given:
        def byteX = new byte[SegmentBatch2.SEGMENT_HEADER_LENGTH]

        when:
        byteX[8] = Chunk.SegmentType.SLIM.val
        then:
        SegmentBatch2.isSegmentBytesSlim(byteX, 0)
        !SegmentBatch2.isSegmentBytesTight(byteX, 0)

        when:
        byteX[8] = Chunk.SegmentType.TIGHT.val
        then:
        !SegmentBatch2.isSegmentBytesSlim(byteX, 0)
        SegmentBatch2.isSegmentBytesTight(byteX, 0)

        when:
        def vList = Mock.prepareValueList(2, 0)
        List<SegmentBatch2.CvWithKeyAndSegmentOffset> invalidCvList = []
        for (v in vList) {
            invalidCvList << new SegmentBatch2.CvWithKeyAndSegmentOffset(Mock.fromV(v), v.key(), (int) v.seq(), 0, (byte) 0)
        }
        println invalidCvList[0].toString()
        println invalidCvList[0].shortString()
        def segmentBytesSlim = SegmentBatch2.encodeValidCvListSlim(invalidCvList)
        then:
        SegmentBatch2.getKeyBytesAndValueBytesInSegmentBytesSlim(segmentBytesSlim, (byte) 0, 0).keyBytes() == vList[0].key().bytes
        SegmentBatch2.getKeyBytesAndValueBytesInSegmentBytesSlim(segmentBytesSlim, (byte) 0, 1).valueBytes() == vList[1].cvEncoded()
        SegmentBatch2.getKeyBytesAndValueBytesInSegmentBytesSlim(segmentBytesSlim, (byte) 0, 2) == null

        when:
        SegmentBatch2.iterateFromSegmentBytes(segmentBytesSlim, new SegmentBatch2.ForDebugCvCallback())
        then:
        1 == 1

        when:
        def cvListMany = Mock.prepareCompressedValueList(100)
        List<SegmentBatch2.CvWithKeyAndSegmentOffset> invalidCvListMany = []
        for (cv in cvListMany) {
            invalidCvListMany << new SegmentBatch2.CvWithKeyAndSegmentOffset(cv, 'key:' + cv.seq, (int) cv.seq, 0, (byte) 0)
        }
        def segmentBytesSlim2 = SegmentBatch2.encodeValidCvListSlim(invalidCvListMany)
        then:
        segmentBytesSlim2 == null
    }

    def 'test read tight segment'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch = new SegmentBatch(slot, snowFlake)
        def segmentBatch2 = new SegmentBatch2(slot, snowFlake)

        and:
        def list = Mock.prepareValueList(800)
        ArrayList<PersistValueMeta> returnPvmList = []

        when:
        def r = segmentBatch.splitAndTight(list, returnPvmList)
        def first = r[0]
        ArrayList<SegmentBatch2.CvWithKeyAndSegmentOffset> cvList = []
        SegmentBatch2.readToCvList(cvList, first.tightBytesWithLength(), 0, 4096, 0, (short) 0)
        then:
        cvList.size() == 268

        when:
        def r2 = segmentBatch2.split(list, returnPvmList)
        def first2 = r2[0]
        ArrayList<SegmentBatch2.CvWithKeyAndSegmentOffset> cvList2 = []
        SegmentBatch2.readToCvList(cvList2, first2.segmentBytes(), 0, 4096, 0, (short) 0)
        then:
        cvList2.size() == 67
    }
}
