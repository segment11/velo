package io.velo.persist.index

import io.activej.config.Config
import io.velo.SnowFlake
import io.velo.persist.Consts
import spock.lang.Specification

class ReverseIndexChunkTest extends Specification {
    def 'test all'() {
        given:
        // no expire for test
        def persistConfig = Config.create().with('expiredIfSecondsFromNow', '3600')
        def reverseIndexChunk = new ReverseIndexChunk((byte) 0, Consts.indexWorkerDir, (byte) 1, persistConfig)

        expect:
        reverseIndexChunk.targetFdIndex(0) == 0
        reverseIndexChunk.targetSegmentIndexTargetFd(0) == 0
        reverseIndexChunk.targetFdIndex(reverseIndexChunk.segmentNumberPerFd) == 1
        reverseIndexChunk.targetSegmentIndexTargetFd(reverseIndexChunk.segmentNumberPerFd) == 0

        when:
        def segmentIndex = reverseIndexChunk.initMetaForOneWord('bad')
        def segmentIndex2 = reverseIndexChunk.initMetaForOneWord('bad')
        then:
        segmentIndex == ReverseIndexChunk.HEADER_USED_SEGMENT_COUNT
        segmentIndex2 == ReverseIndexChunk.HEADER_USED_SEGMENT_COUNT

        when:
        reverseIndexChunk.initMetaForOneWord('xxx')
        def notWriteYetQuerySet = reverseIndexChunk.getLongIds('xxx', 0, 1)
        def notInitMetaYetQuerySet = reverseIndexChunk.getLongIds('yyy', 0, 1)
        then:
        notWriteYetQuerySet.size() == 0
        notInitMetaYetQuerySet.size() == 0

        when:
        def snowFlake = new SnowFlake(1, 1)
        // not init meta
        def exception = false
        try {
            reverseIndexChunk.addLongId('yyy', snowFlake.nextId())
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        10.times {
            // not 0
            reverseIndexChunk.addLongId('bad', snowFlake.nextId())
        }
        then:
        reverseIndexChunk.getLongIds('bad', 0, 20).size() == 10
        reverseIndexChunk.getLongIds('bad', 0, 5).size() == 5
        reverseIndexChunk.getLongIds('bad', 2, 5).size() == 5
        reverseIndexChunk.getLongIds('bad', 6, 5).size() == 4

        when:
        def notExistWordQuerySet = reverseIndexChunk.getLongIds('xxx', 0, 1)
        then:
        notExistWordQuerySet.size() == 0

        when:
        exception = false
        for (i in 0..<32768) {
            try {
                reverseIndexChunk.addLongId('bad', snowFlake.nextId())
            } catch (RuntimeException e) {
                println e.message
                exception = true
                break
            }
        }
        then:
        exception

        when:
        reverseIndexChunk.cleanUp()
        // load again
        def persistConfig2 = Config.create().with('expiredIfSecondsFromNow', '1')
        def reverseIndexChunk2 = new ReverseIndexChunk((byte) 0, Consts.indexWorkerDir, (byte) 1, persistConfig2)
        // wait expired
        Thread.sleep(1000 * 2)
        then:
        reverseIndexChunk2.getLongIds('bad', 0, 5).size() == 0

        when:
        // trigger merged all expired
        reverseIndexChunk2.merge(ReverseIndexChunk.HEADER_USED_SEGMENT_COUNT, snowFlake.nextId())
        then:
        reverseIndexChunk2.getLongIds('bad', 0, 5).size() == 1

        when:
        exception = false
        try {
            reverseIndexChunk2.maxSegmentNumber.times {
                reverseIndexChunk2.initMetaForOneWord('bad' + it)
            }
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            new ReverseIndexChunk((byte) 0, Consts.indexWorkerDir, (byte) (ReverseIndexChunk.MAX_FD_PER_CHUNK + 1), persistConfig)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        reverseIndexChunk2.clear()
        reverseIndexChunk2.cleanUp()
        Consts.persistDir.deleteDir()
    }
}

