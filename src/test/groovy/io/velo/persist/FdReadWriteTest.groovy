package io.velo.persist

import io.velo.ConfForSlot
import org.apache.commons.io.FileUtils
import spock.lang.Specification

class FdReadWriteTest extends Specification {
    def 'test write and read'() {
        given:
        def oneFile1 = new File('/tmp/test-fd-read-write-chunk')
        if (oneFile1.exists()) {
            oneFile1.delete()
        }
        def oneFile2 = new File('/tmp/test-fd-read-write-key-bucket')
        if (!oneFile2.exists()) {
            FileUtils.touch(oneFile2)
        }
        def oneFile11 = new File('/tmp/test-fd-read-write-chunk2')
        if (oneFile11.exists()) {
            oneFile11.delete()
        }
        def oneFile22 = new File('/tmp/test-fd-read-write-key-bucket2')
        if (!oneFile22.exists()) {
            FileUtils.touch(oneFile22)
        }

        // chunk segment length same with one key bucket cost length
        ConfForSlot.global.confChunk.segmentLength = KeyLoader.KEY_BUCKET_ONE_COST_SIZE
        ConfForSlot.global.confChunk.lruPerFd.maxSize = 10
        ConfForSlot.global.confBucket.lruPerFd.maxSize = 10

        and:
        def fdChunk = new FdReadWrite('test', oneFile1)
        fdChunk.initByteBuffers(true, 0)
        println fdChunk
        println 'in memory size estimate: ' + fdChunk.estimate(new StringBuilder())

        def fdKeyBucket = new FdReadWrite('test2', oneFile2)
        fdKeyBucket.initByteBuffers(false, 0)
        println fdKeyBucket
        println 'in memory size estimate: ' + fdKeyBucket.estimate(new StringBuilder())

        fdChunk.collect()
        fdKeyBucket.collect()
        fdChunk.afterReadCompressCountTotal = 1
        fdChunk.afterReadCompressBytesTotalLength = 100
        fdChunk.afterReadCompressedBytesTotalLength = 50
        fdChunk.readCountTotal = 1
        fdChunk.writeCountTotal = 1
        fdChunk.lruHitCounter = 1
        fdChunk.lruMissCounter = 1
        fdKeyBucket.keyBucketSharedBytesCompressCountTotal = 1
        fdKeyBucket.keyBucketSharedBytesDecompressCountTotal = 1
        fdKeyBucket.keyBucketSharedBytesBeforeCompressedBytesTotal = 1000
        fdKeyBucket.keyBucketSharedBytesAfterCompressedBytesTotal = 100
        fdChunk.collect()
        fdKeyBucket.collect()

        def segmentLength = ConfForSlot.global.confChunk.segmentLength
        def oneChargeBucketNumber = ConfForSlot.global.confWal.oneChargeBucketNumber

        when:
        // lru off
        ConfForSlot.global.confChunk.lruPerFd.maxSize = 0
        ConfForSlot.global.confBucket.lruPerFd.maxSize = 0
        def fdChunk11 = new FdReadWrite('test11', oneFile11)
        def fdKeyBucket22 = new FdReadWrite('test22', oneFile22)
        fdChunk11.initByteBuffers(true, 0)
        fdKeyBucket22.initByteBuffers(false, 0)
        then:
        fdChunk11 != null
        fdKeyBucket22 != null

        when:
        int loop = 10
        int[] array = new int[loop * 2]
        loop.times { i ->
            byte[] bytes
            if (i == 0) {
                bytes = new byte[segmentLength - 1]
            } else {
                bytes = new byte[segmentLength]
                Arrays.fill(bytes, (byte) i)
            }

            def f1 = fdChunk.writeOneInner(i, bytes, false)
            def f11 = fdChunk11.writeOneInner(i, bytes, false)
            def f2 = fdKeyBucket.writeOneInner(i, bytes, false)
            if (i == 9) {
                f1 = fdChunk.writeOneInner(i, bytes, true)
                f2 = fdKeyBucket.writeOneInner(i, bytes, true)
            }
            array[i] = f1
            array[i + loop] = f2
        }
        fdKeyBucket.writeSharedBytesForKeyBucketsInOneWalGroup(1 * oneChargeBucketNumber, new byte[oneChargeBucketNumber * segmentLength])
        then:
        array.every { it == segmentLength }
        fdChunk.readOneInner(0, false).length == segmentLength
        fdChunk.readOneInner(0, true).length == segmentLength
        fdChunk.readOneInner(0, true).length == segmentLength
        fdChunk11.readOneInner(0, true).length == segmentLength
        fdChunk.readSegmentsForMerge(0, loop).length == segmentLength * loop
        fdChunk.readBatchForRepl(0).length == segmentLength * loop
        fdKeyBucket.readOneInner(0, false).length == segmentLength
        fdKeyBucket.readOneInner(0, true).length == segmentLength
        fdKeyBucket.readOneInner(0, true).length == segmentLength
        fdKeyBucket.readKeyBucketsSharedBytesInOneWalGroup(1 * oneChargeBucketNumber) != null

        when:
        fdChunk.writeSegmentsBatch(100, new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE], false)
        then:
        fdChunk.readOneInner(100 + FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE - 1, false).length == segmentLength

        when:
        fdChunk.writeSegmentsBatch(100, new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE], true)
        then:
        fdChunk.readOneInner(100 + FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE - 1, false).length == segmentLength

        when:
        boolean exception = false
        try {
            fdChunk.writeSegmentsBatch(0, new byte[segmentLength * 2], true)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdChunk.writeOneInner(0, new byte[segmentLength * 2], true)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdChunk.readOneInner(-1, false)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdChunk.readOneInner(ConfForSlot.global.confChunk.segmentNumberPerFd, false)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.readOneInner(-1, true)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.readOneInner(ConfForSlot.global.confBucket.bucketsPerSlot, true)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdChunk.readSegmentsForMerge(0, FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE + 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def oldWriteIndex = fdChunk.writeIndex
        fdChunk.writeIndex = 1
        def bytesForMerge = fdChunk.readSegmentsForMerge(10, loop)
        fdChunk.writeIndex = oldWriteIndex
        then:
        bytesForMerge == null

        when:
        oldWriteIndex = fdChunk.writeIndex
        fdChunk.writeIndex = 4096
        def bytesX = fdChunk.readSegmentsForMerge(0, 10)
        fdChunk.writeIndex = oldWriteIndex
        then:
        bytesX.length == 4096

        when:
        exception = false
        oldWriteIndex = fdChunk.writeIndex
        fdChunk.writeIndex = 10
        try {
            fdChunk.readSegmentsForMerge(0, 10)
        } catch (RuntimeException e) {
            println e.message
            exception = true
        } finally {
            fdChunk.writeIndex = oldWriteIndex
        }
        then:
        !exception

        cleanup:
        fdChunk.truncate()
        fdChunk.cleanUp()
        fdKeyBucket.truncate()
        fdKeyBucket.cleanUp()
        fdChunk11.truncate()
        fdChunk11.cleanUp()
        fdKeyBucket22.truncate()
        fdKeyBucket22.cleanUp()
        oneFile1.delete()
        oneFile2.delete()
        oneFile11.delete()
        oneFile22.delete()
    }

    def 'test truncate after target segment index'() {
        given:
        def oneFile1 = new File('/tmp/test-fd-read-write-chunk')
        if (oneFile1.exists()) {
            oneFile1.delete()
        }

        def segmentLength = ConfForSlot.global.confChunk.segmentLength

        def fdChunk = new FdReadWrite('test', oneFile1)
        fdChunk.initByteBuffers(true, 0)

        when:
        fdChunk.writeSegmentsBatch(0, new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_WRITE], false)
        fdChunk.truncateAfterTargetSegmentIndex(1)
        then:
        fdChunk.readOneInner(0, false) != null
        fdChunk.readOneInner(1, false) == null

        cleanup:
        fdChunk.truncate()
        fdChunk.cleanUp()
    }

    def 'test warm up'() {
        given:
        def oneFile2 = new File('/tmp/test-fd-read-write-key-bucket')
        if (!oneFile2.exists()) {
            FileUtils.touch(oneFile2)
        }

        ConfForSlot.global.confBucket.lruPerFd.maxSize = 0

        def fdKeyBucket = new FdReadWrite('test2', oneFile2)
        fdKeyBucket.initByteBuffers(false, 0)

        when:
        // no data
        fdKeyBucket.warmUp()
        fdKeyBucket.warmUp()
        then:
        fdKeyBucket.oneInnerBytesByIndexLRU.isEmpty()

        when:
        fdKeyBucket.writeOneInner(0, new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE], false)
        fdKeyBucket.writeOneInner(1, new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE], false)
        def n0 = fdKeyBucket.warmUp()
        then:
        n0 != 0
        fdKeyBucket.oneInnerBytesByIndexLRU.size() == 2

        cleanup:
        fdKeyBucket.truncate()
        fdKeyBucket.cleanUp()
    }
}
