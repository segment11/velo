package io.velo.persist.index

import io.activej.config.Config
import io.velo.persist.Consts
import spock.lang.Specification

class ReverseIndexChunkTest extends Specification {
    def 'test all'() {
        given:
        // no expire for test
        def persistConfig = Config.create().with('expiredIfSecondsFromNow', '0')
        def reverseIndexChunk = new ReverseIndexChunk((byte) 0, Consts.indexWorkerDir, (byte) 1, persistConfig)

        expect:
        reverseIndexChunk.targetFdIndex(0) == 0
        reverseIndexChunk.targetSegmentIndexTargetFd(0) == 0
        reverseIndexChunk.targetFdIndex(reverseIndexChunk.segmentNumberPerFd) == 1
        reverseIndexChunk.targetSegmentIndexTargetFd(reverseIndexChunk.segmentNumberPerFd) == 0

        when:
        def segmentIndex = reverseIndexChunk.initMetaForOneWord('bad')
        then:
        segmentIndex == 4

        when:
        10.times {
            // not 0
            reverseIndexChunk.addLongId('bad', it + 1)
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
        reverseIndexChunk.initMetaForOneWord('xxx')
        def notWriteYetQuerySet = reverseIndexChunk.getLongIds('xxx', 0, 1)
        then:
        notWriteYetQuerySet.size() == 0

        when:
        def exception = false
        try {
            reverseIndexChunk.addLongId('yyy', 1)
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        for (i in 0..<32768) {
            try {
                reverseIndexChunk.addLongId('bad', i + 100)
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
        def reverseIndexChunk2 = new ReverseIndexChunk((byte) 0, Consts.indexWorkerDir, (byte) 1, persistConfig)
        then:
        reverseIndexChunk2.getLongIds('bad', 0, 5).size() == 5

        cleanup:
        reverseIndexChunk2.clear()
        reverseIndexChunk2.cleanUp()
        Consts.indexWorkerDir.deleteDir()
    }
}

