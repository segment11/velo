package io.velo.persist

import io.velo.CompressedValue
import io.velo.ConfForSlot
import io.velo.SnowFlake
import spock.lang.Specification

import java.nio.ByteBuffer

class SegmentBatch2Test extends Specification {
    def 'split segments write and read'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch2 = new SegmentBatch2(snowFlake)

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

    def 'test read tight segment'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch = new SegmentBatch(snowFlake)
        def segmentBatch2 = new SegmentBatch2(snowFlake)

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
        println cvList[0].shortString()
        println cvList[0]
        def r2 = segmentBatch2.split(list, returnPvmList)
        def first2 = r2[0]
        ArrayList<SegmentBatch2.CvWithKeyAndSegmentOffset> cvList2 = []
        SegmentBatch2.readToCvList(cvList2, first2.segmentBytes(), 0, 4096, 0, (short) 0)
        then:
        cvList2.size() == 67
    }

    def 'test segment encode and decode with multibyte key'() {
        given:
        ConfForSlot.global = ConfForSlot.debugMode
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch2 = new SegmentBatch2(snowFlake)
        def key = '中'
        def keyBytes = key.getBytes('UTF-8')
        def keyHash = io.velo.KeyHash.hash(keyBytes)
        def cv = new CompressedValue()
        cv.seq = 1L
        cv.keyHash = keyHash
        cv.dictSeqOrSpType = 1
        cv.compressedData = new byte[10]
        def list = [new Wal.V(1L, 0, keyHash, CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                key, cv.encode(), false)] as ArrayList<Wal.V>
        ArrayList<PersistValueMeta> returnPvmList = []

        when:
        def r = segmentBatch2.split(list, returnPvmList)
        def loadedKeys = []
        SegmentBatch2.iterateFromSegmentBytes(r[0].segmentBytes(), { loadedKey, loadedCv, offsetInThisSegment ->
            loadedKeys << loadedKey
        })

        then:
        loadedKeys == [key]
    }
}
