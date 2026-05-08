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

    def 'chunk persist logs and rethrows key bucket update failure after segment write'() {
        given:
        ConfForSlot.global.confBucket.initialSplitNumber = 1
        def keyLoader = new FailingKeyLoader(slot)
        def oneSlot = new OneSlot(slot, Consts.slotDir, keyLoader, null)
        def chunk = new Chunk(slot, Consts.slotDir, oneSlot)
        oneSlot.chunk = chunk
        chunk.initFds()

        when:
        chunk.persist(0, prepareWalGroup0ValueList(1), null)

        then:
        def e = thrown(RuntimeException)
        e.message == 'mock key bucket update failed'
        oneSlot.metaChunkSegmentFlagSeq.getSegmentMergeFlag(0).flagByte() == Chunk.SEGMENT_FLAG_HAS_DATA

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
