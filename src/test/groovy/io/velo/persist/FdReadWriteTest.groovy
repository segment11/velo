package io.velo.persist

import io.velo.ConfForGlobal
import io.velo.ConfForSlot
import jnr.ffi.LibraryLoader
import jnr.posix.LibC
import org.apache.commons.io.FileUtils
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.file.Paths

class FdReadWriteTest extends Specification {
    static {
        var extendConfigFile = Paths.get('velo_extend.properties').toFile()
        if (!extendConfigFile.exists()) {
            extendConfigFile.createNewFile()
            extendConfigFile.text = 'o_direct=16384'
        }
    }

    final short slot = 0

    def 'test write and read'() {
        given:
        System.setProperty('jnr.ffi.asm.enabled', 'false')
        def libC = LibraryLoader.create(LibC.class).load('c')
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
        ConfForGlobal.pureMemory = false
        ConfForSlot.global.confChunk.lruPerFd.maxSize = 10
        ConfForSlot.global.confBucket.lruPerFd.maxSize = 10

        and:
        def fdChunk = new FdReadWrite(slot, 'test', libC, oneFile1)
        fdChunk.initByteBuffers(true)
        println fdChunk
        println 'in memory size estimate: ' + fdChunk.estimate(new StringBuilder())

        def fdKeyBucket = new FdReadWrite(slot, 'test2', libC, oneFile2)
        fdKeyBucket.initByteBuffers(false)
        def walGroupNumber = Wal.calcWalGroupNumber()
        fdKeyBucket.resetAllBytesByOneWalGroupIndexForKeyBucketOneSplitIndex(walGroupNumber)
        fdKeyBucket.clearAllKeyBucketsInOneWalGroupToMemory(0)
        println fdKeyBucket
        println 'in memory size estimate: ' + fdKeyBucket.estimate(new StringBuilder())

        fdChunk.collect()
        fdKeyBucket.collect()
        fdChunk.afterPreadCompressCountTotal = 1
        fdChunk.afterPreadCompressBytesTotalLength = 100
        fdChunk.afterPreadCompressedBytesTotalLength = 50
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
        def fdChunk11 = new FdReadWrite(slot, 'test11', libC, oneFile11)
        def fdKeyBucket22 = new FdReadWrite(slot, 'test22', libC, oneFile22)
        fdChunk11.initByteBuffers(true)
        fdKeyBucket22.initByteBuffers(false)
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
        fdChunk.writeSegmentsBatch(100, new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE], false)
        then:
        fdChunk.readOneInner(100 + FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE - 1, false).length == segmentLength

        when:
        fdChunk.writeSegmentsBatch(100, new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE], true)
        then:
        fdChunk.readOneInner(100 + FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE - 1, false).length == segmentLength

        when:
        boolean exception = false
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
            fdChunk.readSegmentsForMerge(0, loop + 1)
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
        ConfForGlobal.isUseDirectIO ? exception : !exception

        when:
        fdChunk.writeSegmentsBatchForRepl(1024, new byte[segmentLength * FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD])
        then:
        fdChunk.readOneInner(1024 + FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD - 1, false).length == segmentLength

        when:
        fdChunk.truncate()
        fdChunk.cleanUp()
        fdKeyBucket.truncate()
        fdKeyBucket.cleanUp()

        ConfForGlobal.pureMemory = true
        fdChunk = new FdReadWrite(slot, 'test', libC, oneFile1)
        fdChunk.initByteBuffers(true)
        println 'in memory size estimate: ' + fdChunk.estimate(new StringBuilder())
        fdKeyBucket = new FdReadWrite(slot, 'test2', libC, oneFile2)
        fdKeyBucket.initByteBuffers(false)
        println 'in memory size estimate: ' + fdKeyBucket.estimate(new StringBuilder())
        then:
        fdChunk.isTargetSegmentIndexNullInMemory(0)
        fdChunk.clearTargetSegmentIndexInMemory(0)

        when:
        loop.times { i ->
            def bytes = new byte[segmentLength]
            Arrays.fill(bytes, (byte) i)
            def f1 = fdChunk.writeOneInner(i, bytes, false)
            def f2 = fdKeyBucket.writeOneInner(i, bytes, false)
            array[i] = f1
            array[i + loop] = f2
        }
        fdKeyBucket.writeSharedBytesForKeyBucketsInOneWalGroup(1 * oneChargeBucketNumber, new byte[oneChargeBucketNumber * segmentLength])
        println 'in memory size estimate: ' + fdChunk.estimate(new StringBuilder())
        println 'in memory size estimate: ' + fdKeyBucket.estimate(new StringBuilder())
        then:
        !fdChunk.isTargetSegmentIndexNullInMemory(0)
        array.every { it == segmentLength }
        fdChunk.readOneInner(0, false).length == segmentLength
        fdChunk.readSegmentsForMerge(0, loop).length == segmentLength * loop
        fdChunk.readBatchForRepl(0).length == segmentLength * FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD
        fdKeyBucket.readOneInner(0, false).length == segmentLength * oneChargeBucketNumber
        fdKeyBucket.readKeyBucketsSharedBytesInOneWalGroup(1 * oneChargeBucketNumber).length == segmentLength * oneChargeBucketNumber
        fdKeyBucket.readOneInnerBatchFromMemory(1, 1).length == segmentLength * oneChargeBucketNumber
        fdKeyBucket.readOneInnerBatchFromMemory(1, oneChargeBucketNumber).length == segmentLength * oneChargeBucketNumber

        when:
        exception = false
        try {
            fdChunk.writeOneInner(0, new byte[segmentLength + 1], false)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdChunk.writeSegmentsBatch(0, new byte[segmentLength], false)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.writeSharedBytesForKeyBucketsInOneWalGroup(0, new byte[10])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        fdChunk.writeSegmentsBatch(100, new byte[segmentLength * FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE], false)
        then:
        fdChunk.readOneInner(100 + FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_PWRITE - 1, false).length == segmentLength

        when:
        exception = false
        try {
            fdKeyBucket.readOneInnerBatchFromMemory(1, 2)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.writeOneInnerBatchToMemory(0, new byte[10], 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdKeyBucket.writeOneInnerBatchToMemory(0, new byte[KeyLoader.KEY_BUCKET_ONE_COST_SIZE * 2], 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            fdChunk.writeOneInnerBatchToMemory(1, new byte[4096], 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        fdChunk.writeOneInnerBatchToMemory(200, new byte[10], 0)
        then:
        fdChunk.readOneInner(200, false).length == 10

        when:
        fdKeyBucket.clearOneKeyBucketToMemory(oneChargeBucketNumber * 2)
        fdKeyBucket.clearOneKeyBucketToMemory(1)
        def keyBucket1BytesRead = new byte[segmentLength]
        ByteBuffer.wrap(fdKeyBucket.readOneInner(1, false)).get(segmentLength, keyBucket1BytesRead)
        then:
        keyBucket1BytesRead == new byte[segmentLength]

        when:
        fdKeyBucket.clearKeyBucketsToMemory(oneChargeBucketNumber)
        then:
        fdKeyBucket.readKeyBucketsSharedBytesInOneWalGroup(oneChargeBucketNumber) == null

        // warm up
        when:
        ConfForGlobal.pureMemory = false
        ConfForSlot.global.confBucket.lruPerFd.maxSize = ConfForSlot.global.confBucket.bucketsPerSlot
        fdKeyBucket = new FdReadWrite(slot, 'test2', libC, oneFile2)
        fdKeyBucket.initByteBuffers(false)
        def n = fdKeyBucket.warmUp()
        then:
        n == ConfForSlot.global.confBucket.bucketsPerSlot

        when:
        ConfForGlobal.pureMemory = true
        n = fdKeyBucket.warmUp()
        then:
        n == 0

        when:
        ConfForGlobal.pureMemory = false
        exception = false
        try {
            fdChunk.warmUp()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        ConfForGlobal.pureMemory = true
        ConfForGlobal.isPureMemoryModeKeyBucketsUseCompression = true
        fdKeyBucket.initByteBuffers(false)
        fdKeyBucket.setSharedBytesCompressToMemory(new byte[segmentLength * oneChargeBucketNumber], 0)
        then:
        fdKeyBucket.keyBucketSharedBytesCompressCountTotal == 1
        fdKeyBucket.getSharedBytesDecompressFromMemory(0).length == segmentLength * oneChargeBucketNumber

        cleanup:
        fdChunk.truncate()
        fdChunk.cleanUp()
        fdKeyBucket.truncate()
        fdKeyBucket.cleanUp()
        fdChunk11.initPureMemoryByteArray()
        fdChunk11.truncate()
        fdChunk11.cleanUp()
        fdKeyBucket22.initPureMemoryByteArray()
        fdKeyBucket22.truncate()
        fdKeyBucket22.cleanUp()
        oneFile1.delete()
        oneFile2.delete()
        oneFile11.delete()
        oneFile22.delete()
        ConfForGlobal.pureMemory = false
        ConfForGlobal.isPureMemoryModeKeyBucketsUseCompression = false
    }
}
