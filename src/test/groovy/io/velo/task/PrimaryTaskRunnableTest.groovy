package io.velo.task

import io.activej.eventloop.Eventloop
import spock.lang.Specification

import java.time.Duration

class PrimaryTaskRunnableTest extends Specification {
    def 'test run'() {
        given:
        def primaryTaskRunnable = new PrimaryTaskRunnable((i) -> {
            println i
        })

        and:
        def eventloop = Eventloop.builder()
                .withThreadName('test-primary-task-runnable')
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)

        primaryTaskRunnable.primaryEventloop = eventloop

        when:
        Thread.start {
            Thread.currentThread().sleep(1000 * 2)
            primaryTaskRunnable.stop()
            Thread.currentThread().sleep(1000 * 2)
            eventloop.breakEventloop()
        }

        primaryTaskRunnable.run()
        eventloop.run()

        then:
        1 == 1
    }
}
