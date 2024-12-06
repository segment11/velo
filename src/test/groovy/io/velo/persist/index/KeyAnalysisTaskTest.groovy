package io.velo.persist.index

import io.activej.eventloop.Eventloop
import io.velo.persist.LocalPersist
import spock.lang.Specification

import java.time.Duration

class KeyAnalysisTaskTest extends Specification {
    def 'test all'() {
        given:
        def task = new KeyAnalysisTask(null)

        and:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }

        def localPersist = LocalPersist.instance
        localPersist.addOneSlot((short) 0, eventloop)
        localPersist.debugMode()

        when:
        Thread.sleep(100)
        10.times {
            task.run(it)
        }
        then:
        1 == 1

        cleanup:
        eventloop.breakEventloop()
        localPersist.cleanUp()
    }
}
