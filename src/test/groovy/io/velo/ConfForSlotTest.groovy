package io.velo

import io.velo.persist.KeyBucket
import spock.lang.Specification

class ConfForSlotTest extends Specification {
    def 'test all'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000L)
        def c = ConfForSlot.global
        println c.slaveCanMatchCheckValues()

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
        c.confRepl.iterateKeysOneBatchSize == 10000
        c.confRepl.checkIfValid()
        println c.confRepl

        when:
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
}
