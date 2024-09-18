package io.velo


import spock.lang.Specification

class ConfForSlotTest extends Specification {
    def 'test all'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000)
        def c = ConfForSlot.global
        println c.slaveCanMatchCheckValues()

        c.lruBigString.maxSize == 1000
        c.lruKeyAndCompressedValueEncoded.maxSize == 100_000
        println c

        c.confBucket.bucketsPerSlot == 65536
        c.confBucket.initialSplitNumber >= 1
        c.confBucket.lruPerFd.maxSize == 0
        println c.confBucket

        c.confChunk.segmentNumberPerFd == 256 * 1024
        c.confChunk.fdPerChunk == 1
        c.confChunk.maxSegmentNumber() == 256 * 1024
        c.confChunk.segmentLength == 4096
        c.confChunk.fdPerChunk < ConfForSlot.ConfChunk.MAX_FD_PER_CHUNK
        c.confChunk.lruPerFd.maxSize == 0
        println c.confChunk

        c.confWal.oneChargeBucketNumber == 32
        c.confWal.valueSizeTrigger >= 100
        c.confWal.shortValueSizeTrigger >= 100
        println c.confWal

        c.confRepl.binlogOneSegmentLength == 1024 * 1024
        c.confRepl.binlogOneFileMaxLength == 512 * 1024 * 1024
        c.confRepl.binlogForReadCacheSegmentMaxCount == 100
        c.confRepl.catchUpOffsetMinDiff == 1024 * 1024
        c.confRepl.catchUpIntervalMillis == 200
        println c.confRepl
    }

    def 'test different estimate key number'() {
        given:
        def c100k = ConfForSlot.from(100_000)
        def c1m = ConfForSlot.from(1_000_000)
        def c10m = ConfForSlot.from(10_000_000)
        def c100m = ConfForSlot.from(100_000_000)

        expect:
        c100k == ConfForSlot.debugMode
        c1m == ConfForSlot.c1m
        c10m == ConfForSlot.c10m
        c100m == ConfForSlot.c100m
    }

    def 'test chunk reset by one value length'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000)
        def c = ConfForSlot.global

        c.confChunk.mark()

        when:
        ConfForGlobal.isValueSetUseCompression = true
        c.confChunk.resetByOneValueLength(200)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        ConfForGlobal.isValueSetUseCompression = false
        c.confChunk.resetByOneValueLength(200)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        ConfForGlobal.isValueSetUseCompression = true
        c.confChunk.resetByOneValueLength(500)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        ConfForGlobal.isValueSetUseCompression = false
        c.confChunk.resetByOneValueLength(500)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        ConfForGlobal.isValueSetUseCompression = true
        c.confChunk.resetByOneValueLength(1000)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        ConfForGlobal.isValueSetUseCompression = false
        c.confChunk.resetByOneValueLength(1000)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        ConfForGlobal.isValueSetUseCompression = true
        c.confChunk.resetByOneValueLength(2000)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        ConfForGlobal.isValueSetUseCompression = false
        c.confChunk.resetByOneValueLength(2000)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        ConfForGlobal.isValueSetUseCompression = true
        c.confChunk.resetByOneValueLength(ConfForGlobal.MAX_ESTIMATE_ONE_VALUE_LENGTH)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        ConfForGlobal.isValueSetUseCompression = false
        c.confChunk.resetByOneValueLength(ConfForGlobal.MAX_ESTIMATE_ONE_VALUE_LENGTH)
        println c.confChunk
        then:
        1 == 1

        when:
        c.confChunk.reset()
        boolean exception = false
        try {
            c.confChunk.resetByOneValueLength(ConfForGlobal.MAX_ESTIMATE_ONE_VALUE_LENGTH + 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test wal reset by one value length'() {
        given:
        ConfForSlot.global = ConfForSlot.from(1_000_000)
        def c = ConfForSlot.global

        c.confWal.mark()

        when:
        c.confWal.resetByOneValueLength(200)
        println c.confWal
        then:
        1 == 1

        when:
        c.confWal.reset()
        c.confWal.resetByOneValueLength(500)
        println c.confWal
        then:
        1 == 1

        when:
        c.confWal.reset()
        c.confWal.resetByOneValueLength(1000)
        println c.confWal
        then:
        1 == 1

        when:
        c.confWal.reset()
        c.confWal.resetByOneValueLength(2000)
        println c.confWal
        then:
        1 == 1

        when:
        c.confWal.reset()
        c.confWal.resetByOneValueLength(ConfForGlobal.MAX_ESTIMATE_ONE_VALUE_LENGTH)
        println c.confWal
        then:
        1 == 1

        when:
        c.confWal.reset()
        boolean exception = false
        try {
            c.confWal.resetByOneValueLength(ConfForGlobal.MAX_ESTIMATE_ONE_VALUE_LENGTH + 1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        ConfForGlobal.pureMemory = true
        c.confWal.resetByOneValueLength(100)
        println c.confWal
        then:
        1 == 1

        cleanup:
        ConfForGlobal.pureMemory = false
    }
}
