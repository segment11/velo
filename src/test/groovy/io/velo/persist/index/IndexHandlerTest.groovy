package io.velo.persist.index

import io.activej.eventloop.Eventloop
import spock.lang.Specification

import java.time.Duration

class IndexHandlerTest extends Specification {
    def 'test base'() {
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

        def indexHandler0 = new IndexHandler((byte) 0, (byte) 2, eventloopCurrent)
        def indexHandler1 = new IndexHandler((byte) 1, (byte) 2, eventloop)

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
}
