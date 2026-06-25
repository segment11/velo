package io.velo

import io.velo.persist.KeyBucket
import spock.lang.Specification

class ConfForSlotTest extends Specification {
    def 'test all'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000L)
        def c = ConfForSlot.global
        println c.getSlaveCheckValues()
        println c.generateReplProperties()

        c.lruBigString.maxSize == 1000
        c.lruKeyAndCompressedValueEncoded.maxSize == 100_000
        println c

        c.confBucket.bucketsPerSlot == 65536
        c.confBucket.initialSplitNumber >= 1
        c.confBucket.lruPerFd.maxSize == 0
        c.confBucket.checkIfValid()
        println c.confBucket

        c.confChunk.segmentNumberPerFd == 256 * 1024
        c.confChunk.fdPerChunk == (byte) 1
        c.confChunk.maxSegmentNumber() == 256 * 1024
        c.confChunk.segmentLength == 4096
        c.confChunk.fdPerChunk < ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK
        c.confChunk.lruPerFd.maxSize == 0
        c.confChunk.checkIfValid()
        println c.confChunk

        c.confWal.oneChargeBucketNumber == 32
        c.confWal.valueSizeTrigger >= 100
        c.confWal.shortValueSizeTrigger >= 100
        c.confWal.atLeastDoPersistOnceIntervalMs >= 1
        c.confWal.checkAtLeastDoPersistOnceSizeRate >= 0
        c.confWal.checkIfValid()
        println c.confWal

        c.confRepl.binlogOneSegmentLength == 1024 * 1024
        c.confRepl.binlogOneFileMaxLength == 512 * 1024 * 1024
        c.confRepl.binlogForReadCacheSegmentMaxCount == (short) 100
        c.confRepl.binlogFileKeepMaxCount == (short) 10
        c.confRepl.catchUpOffsetMinDiff == 1024 * 1024
        c.confRepl.catchUpIntervalMillis == 100
        c.confRepl.checkIfValid()
        println c.confRepl

        when:
        // begin next assert
        boolean exception = false
        c.confBucket.bucketsPerSlot = KeyBucket.MAX_BUCKETS_PER_SLOT * 2
        try {
            c.confBucket.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confBucket.bucketsPerSlot = 1024 + 512
        try {
            c.confBucket.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confBucket.bucketsPerSlot = KeyBucket.MAX_BUCKETS_PER_SLOT
        c.confBucket.initialSplitNumber = (byte) 0
        try {
            c.confBucket.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confBucket.initialSplitNumber = (byte) 9
        c.confBucket.checkIfValid()
        then:
        !exception

        when:
        exception = false
        c.confBucket.initialSplitNumber = (byte) 1
        c.confBucket.onceScanMaxLoopCount = 0
        try {
            c.confBucket.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confBucket.onceScanMaxLoopCount = 1024 * 2
        try {
            c.confBucket.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confChunk.fdPerChunk = 32
        try {
            c.confChunk.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confChunk.fdPerChunk = 16
        c.confChunk.segmentLength = 1024
        try {
            c.confChunk.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confChunk.segmentLength = 4096
        ConfForGlobal.estimateOneValueLength = 1000
        try {
            c.confChunk.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        ConfForGlobal.estimateOneValueLength = 200
        c.confChunk.segmentNumberPerFd = 1024
        try {
            c.confChunk.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confChunk.segmentNumberPerFd = 256 * 1024
        c.confChunk.onceReadSegmentCountWhenRepl = 1024
        try {
            c.confChunk.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confChunk.segmentNumberPerFd = 256 * 1024
        c.confWal.oneChargeBucketNumber = 8
        try {
            c.confWal.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        c.confWal.oneChargeBucketNumber = 32
        c.confWal.onceScanMaxLoopCount = 1024 * 2
        try {
            c.confWal.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            c.confRepl.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        !exception
    }

    def 'test different estimate key number'() {
        given:
        def c100k = ConfForSlot.from(100_000)
        def c1m = ConfForSlot.from(1_000_000)
        def c10m = ConfForSlot.from(10_000_000)

        expect:
        c100k == ConfForSlot.debugMode
        c1m == ConfForSlot.c1m
        c10m == ConfForSlot.c10m
    }

    def 'test slave check values'() {
        given:
        def c = ConfForSlot.from(100_000)

        when:
        def slaveCanMatchResult = c.slaveCanMatch(c.getSlaveCheckValues())
        then:
        slaveCanMatchResult

        when:
        def mapRemote = c.getSlaveCheckValues()
        println mapRemote.datacenterId
        println mapRemote.machineId
        println mapRemote.currentTimeMillis
        println mapRemote.slotNumber
        Thread.sleep(200)
        // time diff too long
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        !slaveCanMatchResult

        when:
        mapRemote = c.getSlaveCheckValues()
        ConfForGlobal.datacenterId++
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        !slaveCanMatchResult

        when:
        mapRemote = c.getSlaveCheckValues()
        ConfForGlobal.machineId++
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        !slaveCanMatchResult

        when:
        mapRemote = c.getSlaveCheckValues()
        ConfForGlobal.slotNumber++
        // slot number slave can > master
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        slaveCanMatchResult

        when:
        mapRemote = c.getSlaveCheckValues()
        ConfForGlobal.slotNumber--
        // slot number slave can not < master
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        !slaveCanMatchResult

        // scale-up topology: slave slotNumber must be exactly N or 2N of master
        when: 'local 4, remote 4 -> accepted (equal)'
        ConfForGlobal.slotNumber = 4
        mapRemote = c.getSlaveCheckValues()
        mapRemote.slotNumber = 4
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        slaveCanMatchResult

        when: 'local 8, remote 4 -> accepted (2N scale-up)'
        ConfForGlobal.slotNumber = 8
        mapRemote = c.getSlaveCheckValues()
        mapRemote.slotNumber = 4
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        slaveCanMatchResult

        when: 'local 5, remote 4 -> rejected (not exactly 2N)'
        ConfForGlobal.slotNumber = 5
        mapRemote = c.getSlaveCheckValues()
        mapRemote.slotNumber = 4
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        !slaveCanMatchResult

        when: 'local 3, remote 4 -> rejected (slave < master)'
        ConfForGlobal.slotNumber = 3
        mapRemote = c.getSlaveCheckValues()
        mapRemote.slotNumber = 4
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        !slaveCanMatchResult

        when: 'local 16, remote 4 -> rejected (4N, not 2N)'
        ConfForGlobal.slotNumber = 16
        mapRemote = c.getSlaveCheckValues()
        mapRemote.slotNumber = 4
        slaveCanMatchResult = c.slaveCanMatch(mapRemote)
        then:
        !slaveCanMatchResult

        cleanup:
        // reset back for last assert
        ConfForGlobal.datacenterId = 0L
        ConfForGlobal.machineId = 0L
        ConfForGlobal.slotNumber = 1
    }

    def 'test chunk segment number per fd must align to truncate check group size'() {
        given:
        def c = ConfForSlot.c1m.confChunk
        def oldSegmentNumberPerFd = c.segmentNumberPerFd
        def oldFdPerChunk = c.fdPerChunk
        def oldSegmentLength = c.segmentLength
        def oldOnceReadSegmentCountWhenRepl = c.onceReadSegmentCountWhenRepl
        def oldEstimateOneValueLength = ConfForGlobal.estimateOneValueLength
        def oldEstimateKeyNumber = ConfForGlobal.estimateKeyNumber

        and:
        c.segmentNumberPerFd = 256 * 1024 + 512
        c.fdPerChunk = 1
        c.segmentLength = 4096
        c.onceReadSegmentCountWhenRepl = 64
        ConfForGlobal.estimateOneValueLength = 200
        ConfForGlobal.estimateKeyNumber = 1_000_000L

        when:
        c.checkIfValid()

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('multiple of 1024')

        cleanup:
        c.segmentNumberPerFd = oldSegmentNumberPerFd
        c.fdPerChunk = oldFdPerChunk
        c.segmentLength = oldSegmentLength
        c.onceReadSegmentCountWhenRepl = oldOnceReadSegmentCountWhenRepl
        ConfForGlobal.estimateOneValueLength = oldEstimateOneValueLength
        ConfForGlobal.estimateKeyNumber = oldEstimateKeyNumber
    }

    def 'test conf repl checkIfValid rejects invalid values'() {
        given:
        def repl = ConfForSlot.c1m.confRepl
        def savedSegmentLength = repl.binlogOneSegmentLength
        def savedFileMaxLength = repl.binlogOneFileMaxLength
        def savedCacheMaxCount = repl.binlogForReadCacheSegmentMaxCount
        def savedKeepMaxCount = repl.binlogFileKeepMaxCount
        def savedOffsetMinDiff = repl.catchUpOffsetMinDiff
        def savedIntervalMs = repl.catchUpIntervalMillis

        when: 'segment length <= 0'
        repl.binlogOneSegmentLength = 0
        repl.checkIfValid()
        then:
        def e1 = thrown(IllegalArgumentException)
        e1.message.contains('segment length')

        when: 'segment length > file max length'
        repl.binlogOneSegmentLength = 999
        repl.binlogOneFileMaxLength = 100
        repl.checkIfValid()
        then:
        def e2 = thrown(IllegalArgumentException)
        e2.message.contains('segment length')

        when: 'cache segment max count <= 0'
        repl.binlogOneSegmentLength = 1024 * 1024
        repl.binlogOneFileMaxLength = 512 * 1024 * 1024
        repl.binlogForReadCacheSegmentMaxCount = (short) 0
        repl.checkIfValid()
        then:
        def e3 = thrown(IllegalArgumentException)
        e3.message.contains('cache segment')

        when: 'file keep max count <= 0'
        repl.binlogForReadCacheSegmentMaxCount = (short) 10
        repl.binlogFileKeepMaxCount = (short) 0
        repl.checkIfValid()
        then:
        def e4 = thrown(IllegalArgumentException)
        e4.message.contains('file keep')

        when: 'catch up offset min diff <= 0'
        repl.binlogFileKeepMaxCount = (short) 10
        repl.catchUpOffsetMinDiff = 0
        repl.checkIfValid()
        then:
        def e5 = thrown(IllegalArgumentException)
        e5.message.contains('offset')

        when: 'catch up interval millis <= 0'
        repl.catchUpOffsetMinDiff = 1024 * 1024
        repl.catchUpIntervalMillis = 0
        repl.checkIfValid()
        then:
        def e6 = thrown(IllegalArgumentException)
        e6.message.contains('interval')

        when: 'all valid values pass'
        repl.catchUpIntervalMillis = 100
        repl.checkIfValid()
        then:
        noExceptionThrown()

        cleanup:
        repl.binlogOneSegmentLength = savedSegmentLength
        repl.binlogOneFileMaxLength = savedFileMaxLength
        repl.binlogForReadCacheSegmentMaxCount = savedCacheMaxCount
        repl.binlogFileKeepMaxCount = savedKeepMaxCount
        repl.catchUpOffsetMinDiff = savedOffsetMinDiff
        repl.catchUpIntervalMillis = savedIntervalMs
    }
}
