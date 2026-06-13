package io.velo.persist

import io.velo.CompressedValue
import io.velo.ConfForSlot
import io.velo.KeyHash
import io.velo.SnowFlake
import spock.lang.Specification

class ChunkWalGroupRebuildTest extends Specification {
    final short slot = 0

    static class FailingKeyLoader extends KeyLoader {
        FailingKeyLoader(short slot) {
            super(slot, ConfForSlot.global.confBucket.bucketsPerSlot, Consts.slotDir, new SnowFlake(1, 1))
        }

        @Override
        void updatePvmListBatchAfterWriteSegments(int walGroupIndex,
                                                  ArrayList<PersistValueMeta> pvmList,
                                                  KeyBucketsInOneWalGroup keyBucketsInOneWalGroupGiven) {
            throw new RuntimeException('mock key bucket update failed')
        }
    }

    private static ArrayList<Wal.V> prepareWalGroup0ValueList(int n) {
        def r = [] as ArrayList<Wal.V>
        int i = 0
        while (r.size() < n) {
            def key = 'rebuild-key:' + i
            def keyHash = KeyHash.hash(key.bytes)
            def bucketIndex = KeyHash.bucketIndex(keyHash)
            if (Wal.calcWalGroupIndex(bucketIndex) == 0) {
                def cv = new CompressedValue()
                cv.seq = r.size() + 1L
                cv.keyHash = keyHash
                cv.compressedData = ('value:' + i).bytes
                r << new Wal.V(cv.seq, bucketIndex, keyHash, CompressedValue.NO_EXPIRE,
                        CompressedValue.NULL_DICT_SEQ, key, cv.encode(), false)
            }
            i++
        }
        r
    }

    private static Wal.V prepareWalGroup0Value(String key, long seq, byte[] valueBytes) {
        def keyHash = KeyHash.hash(key.bytes)
        def bucketIndex = KeyHash.bucketIndex(keyHash)
        assert Wal.calcWalGroupIndex(bucketIndex) == 0
        def cv = new CompressedValue()
        cv.seq = seq
        cv.keyHash = keyHash
        cv.compressedData = valueBytes
        new Wal.V(seq, bucketIndex, keyHash, CompressedValue.NO_EXPIRE,
                CompressedValue.NULL_DICT_SEQ, key, cv.encode(), false)
    }

    private static Wal.V prepareWalGroup0ShortValue(String key, long seq, byte[] valueBytes) {
        def keyHash = KeyHash.hash(key.bytes)
        def bucketIndex = KeyHash.bucketIndex(keyHash)
        assert Wal.calcWalGroupIndex(bucketIndex) == 0
        def cv = new CompressedValue()
        cv.seq = seq
        cv.expireAt = CompressedValue.NO_EXPIRE
        cv.compressedData = valueBytes
        new Wal.V(seq, bucketIndex, keyHash, CompressedValue.NO_EXPIRE,
                CompressedValue.SP_TYPE_SHORT_STRING, key, cv.encodeAsShortString(), false)
    }

    def 'preview scans chunk without mutating key buckets and apply rebuilds from chunk'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        def chunk = ChunkTest.prepareOne(slot, true)
        def oneSlot = chunk.oneSlot
        chunk.initFds()

        and:
        def vList = prepareWalGroup0ValueList(20)
        chunk.segmentIndex = 0
        chunk.persist(0, vList, null)

        and:
        def first = vList[0]
        oneSlot.keyLoader.replacePvmListBatchInOneWalGroupForRebuild(0, new ArrayList<PersistValueMeta>())
        oneSlot.keyLoader.getValueXByKey(first.bucketIndex(), first.key(), first.keyHash()) == null

        when:
        def preview = oneSlot.rebuildKeyBucketsFromChunk(0, ChunkWalGroupRebuilder.Mode.PREVIEW)

        then:
        !preview.applied()
        preview.segmentsScanned() > 0
        preview.recordsDecoded() == vList.size()
        preview.recordsAfterSeqMerge() == vList.size()
        oneSlot.keyLoader.getValueXByKey(first.bucketIndex(), first.key(), first.keyHash()) == null

        when:
        def applied = oneSlot.rebuildKeyBucketsFromChunk(0, ChunkWalGroupRebuilder.Mode.APPLY)
        def valueAfterApply = oneSlot.keyLoader.getValueXByKey(first.bucketIndex(), first.key(), first.keyHash())

        then:
        applied.applied()
        applied.recordsAfterSeqMerge() == vList.size()
        valueAfterApply != null
        PersistValueMeta.isPvm(valueAfterApply.valueBytes())

        cleanup:
        chunk.cleanUp()
        oneSlot.keyLoader.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'rebuild keeps latest sequence per key from chunk records'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        def chunk = ChunkTest.prepareOne(slot, true)
        def oneSlot = chunk.oneSlot
        chunk.initFds()

        and:
        def key = (0..100000).find { candidate ->
            def candidateKey = 'key:latest-seq:' + candidate
            Wal.calcWalGroupIndex(KeyHash.bucketIndex(KeyHash.hash(candidateKey.bytes))) == 0
        }.with { 'key:latest-seq:' + it }
        def keyHash = KeyHash.hash(key.bytes)
        def vOld = prepareWalGroup0Value(key, 1L, 'old'.bytes)
        def vNew = prepareWalGroup0Value(key, 2L, 'new'.bytes)
        chunk.segmentIndex = 0
        chunk.persist(0, [vOld] as ArrayList<Wal.V>, null)
        chunk.persist(0, [vNew] as ArrayList<Wal.V>, null)
        oneSlot.keyLoader.replacePvmListBatchInOneWalGroupForRebuild(0, new ArrayList<PersistValueMeta>())

        when:
        def applied = oneSlot.rebuildKeyBucketsFromChunk(0, ChunkWalGroupRebuilder.Mode.APPLY)
        def valueAfterApply = oneSlot.keyLoader.getValueXByKey(KeyHash.bucketIndex(keyHash), key, keyHash)
        def pvm = PersistValueMeta.decode(valueAfterApply.valueBytes())
        def segmentBytes = oneSlot.chunk.readOneSegment(pvm.segmentIndex)
        def cvList = [] as ArrayList<SegmentBatch2.CvWithKeyAndSegmentOffset>
        SegmentBatch2.readToCvList(cvList, segmentBytes, 0, ConfForSlot.global.confChunk.segmentLength, pvm.segmentIndex, slot)

        then:
        applied.recordsDecoded() == 2
        applied.recordsAfterSeqMerge() == 1
        applied.staleRecords() == 1
        cvList.find { it.key() == key && it.segmentOffset() == pvm.segmentOffset }.cv().seq == 2L

        cleanup:
        chunk.cleanUp()
        oneSlot.keyLoader.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'rebuild preserves short values already stored in key buckets'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        def chunk = ChunkTest.prepareOne(slot, true)
        def oneSlot = chunk.oneSlot
        chunk.initFds()

        and:
        def valueKey = (0..100000).find { candidate ->
            def candidateKey = 'rebuild-short-preserve-key:' + candidate
            Wal.calcWalGroupIndex(KeyHash.bucketIndex(KeyHash.hash(candidateKey.bytes))) == 0
        }.with { 'rebuild-short-preserve-key:' + it }
        def value = prepareWalGroup0Value(valueKey, 1L, 'chunk-value'.bytes)
        def shortValue = (0..100000).find { candidate ->
            def candidateKey = 'short-key:' + candidate
            Wal.calcWalGroupIndex(KeyHash.bucketIndex(KeyHash.hash(candidateKey.bytes))) == 0
        }.with { prepareWalGroup0ShortValue('short-key:' + it, 10L, 'short-value'.bytes) }
        chunk.segmentIndex = 0
        chunk.persist(0, [value] as ArrayList<Wal.V>, null)
        oneSlot.keyLoader.persistShortValueListBatchInOneWalGroup(0, [shortValue])
        oneSlot.keyLoader.getValueXByKey(shortValue.bucketIndex(), shortValue.key(), shortValue.keyHash()).valueBytes() == shortValue.cvEncoded()

        when:
        def applied = oneSlot.rebuildKeyBucketsFromChunk(0, ChunkWalGroupRebuilder.Mode.APPLY)
        def chunkValueAfterApply = oneSlot.keyLoader.getValueXByKey(value.bucketIndex(), value.key(), value.keyHash())
        def shortValueAfterApply = oneSlot.keyLoader.getValueXByKey(shortValue.bucketIndex(), shortValue.key(), shortValue.keyHash())

        then:
        applied.applied()
        PersistValueMeta.isPvm(chunkValueAfterApply.valueBytes())
        shortValueAfterApply.valueBytes() == shortValue.cvEncoded()

        cleanup:
        chunk.cleanUp()
        oneSlot.keyLoader.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'chunk persist rolls back single segment flag and rewinds segment index when key bucket update fails'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        def keyLoader = new FailingKeyLoader(slot)
        def oneSlot = new OneSlot(slot, Consts.slotDir, keyLoader, null)
        def chunk = new Chunk(slot, Consts.slotDir, oneSlot)
        oneSlot.chunk = chunk
        chunk.initFds()

        and: 'single value produces a single segment (segmentCount < BATCH_ONCE_SEGMENT_COUNT_WRITE)'
        def vList = prepareWalGroup0ValueList(1)
        def pvmListProbe = [] as ArrayList<PersistValueMeta>
        def segmentCount = (ConfForSlot.global.confChunk.isSegmentUseCompression ?
                new SegmentBatch(new SnowFlake(1, 1)) :
                new SegmentBatch2(new SnowFlake(1, 1))).split(vList, pvmListProbe).size()
        assert segmentCount == 1
        assert segmentCount < FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE

        when:
        chunk.persist(0, vList, null)

        then: 'persist rethrows the bucket-update failure'
        def e = thrown(RuntimeException)
        e.message == 'mock key bucket update failed'

        and: 'segment 0 is rolled back to REUSABLE (flag, seq cleared)'
        def flag0 = oneSlot.metaChunkSegmentFlagSeq.getSegmentMergeFlag(0)
        flag0.flagByte() == Chunk.SEGMENT_FLAG_REUSABLE
        flag0.segmentSeq() == 0L

        and: 'no spurious WAL-group marker was inserted for the rolled-back range'
        oneSlot.metaChunkSegmentFlagSeq.countMarkersForWalGroup(0) == 0

        and: 'rolled-back segment is immediately reusable via findCanReuseSegmentIndex'
        oneSlot.metaChunkSegmentFlagSeq.findCanReuseSegmentIndex(0, 1) == 0

        and: 'in-memory chunk.segmentIndex was rewound to currentSegmentIndex (0), not advanced past the rolled-back segment'
        chunk.segmentIndex == 0

        cleanup:
        chunk.cleanUp()
        keyLoader.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'chunk persist rolls back batch-path segment flags when key bucket update fails'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        def keyLoader = new FailingKeyLoader(slot)
        def oneSlot = new OneSlot(slot, Consts.slotDir, keyLoader, null)
        def chunk = new Chunk(slot, Consts.slotDir, oneSlot)
        oneSlot.chunk = chunk
        chunk.initFds()

        and: 'enough values to force the batch write branch (segmentCount >= BATCH_ONCE_SEGMENT_COUNT_WRITE) plus a remainder'
        def segmentBatch = ConfForSlot.global.confChunk.isSegmentUseCompression ?
                new SegmentBatch(new SnowFlake(1, 1)) :
                new SegmentBatch2(new SnowFlake(1, 1))
        def vList = (50..2000).step(50).findResult { n ->
            def candidate = prepareWalGroup0ValueList(n)
            def pvmListProbe = [] as ArrayList<PersistValueMeta>
            def cnt = segmentBatch.split(candidate, pvmListProbe).size()
            cnt >= FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE + 1 ? [candidate, cnt] : null
        }
        assert vList != null
        def values = vList[0] as ArrayList<Wal.V>
        def expectedSegmentCount = (int) vList[1]
        assert expectedSegmentCount >= FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE + 1

        when:
        chunk.persist(0, values, null)

        then:
        def e = thrown(RuntimeException)
        e.message == 'mock key bucket update failed'

        and: 'every segment touched in Phase 1 (both batch and remainder paths) is rolled back to REUSABLE'
        (0..<expectedSegmentCount).every {
            def flag = oneSlot.metaChunkSegmentFlagSeq.getSegmentMergeFlag(it)
            flag.flagByte() == Chunk.SEGMENT_FLAG_REUSABLE && flag.segmentSeq() == 0L
        }

        and: 'no spurious WAL-group marker was inserted'
        oneSlot.metaChunkSegmentFlagSeq.countMarkersForWalGroup(0) == 0

        and: 'the entire rolled-back range is reusable as one contiguous block'
        oneSlot.metaChunkSegmentFlagSeq.findCanReuseSegmentIndex(0, expectedSegmentCount) == 0

        and: 'in-memory chunk.segmentIndex was rewound to 0, not advanced past the rolled-back range'
        chunk.segmentIndex == 0

        cleanup:
        chunk.cleanUp()
        keyLoader.cleanUp()
        oneSlot.metaChunkSegmentFlagSeq.clear()
        oneSlot.metaChunkSegmentFlagSeq.cleanUp()
        Consts.slotDir.deleteDir()
    }

    def 'rebuild key buckets from chunk rejects out of range wal group index'() {
        given:
        def oneSlot = new OneSlot(slot)

        when:
        oneSlot.rebuildKeyBucketsFromChunk(Wal.calcWalGroupNumber(), ChunkWalGroupRebuilder.Mode.PREVIEW)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('Wal group index out of range')
    }
}
