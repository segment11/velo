package io.velo.task

import io.activej.eventloop.Eventloop
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class PrimaryTaskRunnableTest extends Specification {
    def 'test stopped runnable does not execute task callback'() {
        given:
        def callCount = new AtomicInteger()
        def primaryTaskRunnable = new PrimaryTaskRunnable((i) -> {
            callCount.incrementAndGet()
        })

        when:
        primaryTaskRunnable.stop()
        primaryTaskRunnable.run()

        then:
        callCount.get() == 0
    }

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
