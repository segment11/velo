package io.velo.persist.index

import io.activej.eventloop.Eventloop
import io.velo.persist.Consts
import spock.lang.Specification

import java.time.Duration

class IndexHandlerTest extends Specification {
    def 'test run'() {
        given:
        def eventloopCurrent = Eventloop.builder()
                .withThreadName('test-index-handler0')
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        def eventloop = Eventloop.builder()
                .withThreadName('test-index-handler1')
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }

        def indexHandler0 = new IndexHandler((byte) 0, eventloopCurrent)
        def indexHandler1 = new IndexHandler((byte) 1, eventloop)

        when:
        indexHandler0.threadIdProtectedForSafe = Thread.currentThread().threadId()
        then:
        indexHandler0.threadIdProtectedForSafe != 0

        when:
        def p0 = indexHandler0.asyncRun {
            println 'async run in index handler 0'
        }
        eventloopCurrent.run()
        then:
        p0.result

        when:
        def p0e = indexHandler0.asyncRun {
            println 'async run but error in index handler 0'
            throw new RuntimeException('error')
        }
        eventloopCurrent.run()
        then:
        p0e.exception != null

        when:
        boolean result = false
        indexHandler1.asyncRun {
            println 'async run in index handler 1'
            result = true
        }
        Thread.sleep(200)
        then:
        result

        cleanup:
        indexHandler0.cleanUp()
        indexHandler1.cleanUp()
    }

    def 'test put word'() {
        given:
        def indexHandler = new IndexHandler((byte) 0, null)

        when:
        boolean exception = false
        try {
            indexHandler.checkWordLength('ab')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        // ab is valid word
        !exception

        when:
        exception = false
        try {
            indexHandler.checkWordLength('a')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            indexHandler.checkWordLength('long_long_long_long_long_long_long_long_long')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test put'() {
        given:
        def indexHandler = new IndexHandler((byte) 0, null)

        and:
        indexHandler.initChunk((byte) 1, Consts.indexWorkerDir, 0)
        expect:
        indexHandler.chunkMaxSegmentNumber > 0

        when:
        indexHandler.putWordIfNotExist('bad')
        then:
        1 == 1

        when:
        indexHandler.addLongId('bad', 1L)
        then:
        indexHandler.getLongIds('bad', 0, 10).size() == 1

        when:
        indexHandler.putWordAndAddLongId('bad', 2L)
        then:
        indexHandler.getTotalCount('bad') == 2

        cleanup:
        indexHandler.cleanUp()
        Consts.indexWorkerDir.deleteDir()
    }

    def 'test repl'() {
        given:
        def indexHandler = new IndexHandler((byte) 0, null)

        and:
        indexHandler.initChunk((byte) 1, Consts.indexWorkerDir, 0)

        when:
        def bytes = indexHandler.metaIndexWordsReadOneBatch(0, 1024)
        then:
        bytes.length == 1024

        when:
        indexHandler.metaIndexWordsWriteOneBatch(0, bytes)
        then:
        1 == 1

        when:
        def bytes1 = indexHandler.chunkReadOneSegment(0)
        then:
        bytes1.length == 1

        when:
        indexHandler.reverseIndexChunk.setMinLength(ReverseIndexChunk.ONE_WORD_HOLD_ONE_SEGMENT_LENGTH)
        bytes1 = indexHandler.chunkReadOneSegment(0)
        then:
        bytes1.length == ReverseIndexChunk.ONE_WORD_HOLD_ONE_SEGMENT_LENGTH

        when:
        indexHandler.chunkWriteOneSegment(0, bytes1)
        then:
        1 == 1

        when:
        indexHandler.resetAsMaster()
        then:
        1 == 1

        cleanup:
        indexHandler.cleanUp()
        Consts.indexWorkerDir.deleteDir()
    }
}
