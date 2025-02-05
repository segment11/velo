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

        println new SegmentBatch2.SegmentBytesWithIndex(new byte[10], 0, 10L)
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
        def bytesX = new byte[16]
        List<CompressedValue> loaded2 = []
        SegmentBatch2.iterateFromSegmentBytes(bytesX, { key, cv, offsetInThisSegment ->
            println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
            loaded2 << cv
        })
        then:
        loaded2.size() == 0

        when:
        bytesX = new byte[18]
        SegmentBatch2.iterateFromSegmentBytes(bytesX, { key, cv, offsetInThisSegment ->
            println "key: $key, cv: $cv, offset in this segment: $offsetInThisSegment"
            loaded2 << cv
        })
        then:
        loaded2.size() == 0

        when:
        boolean exception = false
        ByteBuffer.wrap(bytesX).putShort(16, (short) -1)
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
}
