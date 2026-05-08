package io.velo.persist

import io.velo.CompressedValue
import io.velo.ConfForSlot
import io.velo.KeyHash
import io.velo.SnowFlake
import spock.lang.Specification

import java.nio.ByteBuffer

class SegmentBatchTest extends Specification {
    final short slot = 0

    def 'tight segments write and read'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch = new SegmentBatch(snowFlake)

        segmentBatch.collect()
        segmentBatch.compressCountTotal = 1
        segmentBatch.compressBytesTotal = 1
        segmentBatch.batchCountTotal = 1
        segmentBatch.afterTightSegmentCountTotal = 1
        segmentBatch.collect()

        println new SegmentBatch.SegmentCompressedBytesWithIndex(new byte[10], 0, 10L, 100, false)
        new SegmentBatch2.ForDebugCvCallback().callback('a', new CompressedValue(), 0)

        and:
        def list = Mock.prepareValueList(800)

        ArrayList<PersistValueMeta> returnPvmList = []
        ArrayList<PersistValueMeta> returnPvmList2 = []

        expect:
        SegmentBatch.subBlockMetaPosition(0) == 13
        SegmentBatch.subBlockMetaPosition(1) == 17
        SegmentBatch.subBlockMetaPosition(2) == 21
        SegmentBatch.subBlockMetaPosition(3) == 25

        when:
        def r = segmentBatch.splitAndTight(list, returnPvmList)
        for (one in r) {
            println one
        }
        def r2 = segmentBatch.split(list, [])
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
        pvm0.segmentIndex = 0
        def mockChunk = ChunkTest.prepareOne(slot)
        for (i in 0..<SegmentBatch.MAX_BLOCK_NUMBER) {
            pvm0.subBlockIndex = (byte) i
            def decompressedSegmentBytes0 = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, first.tightBytesWithLength(), pvm0, mockChunk)
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

    def 'tight segments do not overflow chunkSegmentLength on incompressible data'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch = new SegmentBatch(snowFlake)
        def chunkSegmentLength = ConfForSlot.global.confChunk.segmentLength

        // Create entries with large random (incompressible) compressed data.
        // Fill the raw segment as close to chunkSegmentLength as possible
        // so that Zstd compression output exceeds chunkSegmentLength - HEADER_LENGTH.
        // Each entry: persistLength = 2 (key header) + keyBytes.length + cvEncoded.length
        // cvEncoded = 32 (VALUE_HEADER) + compressedData.length
        def random = new Random(42)
        def list = new ArrayList<Wal.V>()
        def seq = 0L
        def keyLen = 16
        def cvHeaderLen = 32
        def entryOverhead = 2 + keyLen + cvHeaderLen
        def compressedDataLength = chunkSegmentLength - SegmentBatch2.SEGMENT_HEADER_LENGTH - entryOverhead - 1
        // One entry fills the segment almost completely (persistLength < chunkSegmentLength)
        def key = 'incomp:' + '0' * (keyLen - 7)
        def keyBytes = key.bytes
        def keyHash = KeyHash.hash(keyBytes)
        def cv = new CompressedValue()
        cv.seq = seq++
        cv.dictSeqOrSpType = CompressedValue.NULL_DICT_SEQ
        cv.keyHash = keyHash
        def data = new byte[compressedDataLength]
        random.nextBytes(data)
        cv.compressedData = data
        list << new Wal.V(cv.seq, 0, keyHash, CompressedValue.NO_EXPIRE,
                CompressedValue.NULL_DICT_SEQ, key, cv.encode(), false)

        ArrayList<PersistValueMeta> returnPvmList = []

        when:
        def r = segmentBatch.splitAndTight(list, returnPvmList)

        then:
        // Every tight segment must fit within chunkSegmentLength bytes
        // (this is what FdReadWrite.writeOneInner checks)
        r.every { one ->
            one.tightBytesWithLength().length <= chunkSegmentLength
        }

        and:
        // No empty tight segments (blockNumber should be > 0)
        r.every { one ->
            one.blockNumber() > 0
        }

        and:
        // All entries should be accounted for in PVMs
        returnPvmList.size() == list.size()

        when:
        // Read back through decompress path to cover raw NORMAL branch
        def mockChunk = ChunkTest.prepareOne(slot)
        def rawSegment = r[0]
        def pvm = returnPvmList[0]
        def decompressed = SegmentBatch.decompressSegmentBytesFromOneSubBlock(slot, rawSegment.tightBytesWithLength(), pvm, mockChunk)

        then:
        decompressed != null
        decompressed.length == chunkSegmentLength

        when:
        // Verify the entry can be decoded from the raw segment bytes
        def loadedCvList = []
        SegmentBatch2.iterateFromSegmentBytes(decompressed, 0, decompressed.length, { k, cvIter, offset ->
            loadedCvList << cvIter
        })

        then:
        loadedCvList.size() == 1
        loadedCvList[0].seq == list[0].seq()
        loadedCvList[0].keyHash == list[0].keyHash()
    }
}
