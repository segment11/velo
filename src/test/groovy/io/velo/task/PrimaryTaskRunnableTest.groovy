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

    def 'test scheduler keeps running after the task callback throws'() {
        given:
        def callCount = new AtomicInteger()
        def primaryTaskRunnable = new PrimaryTaskRunnable((i) -> {
            if (callCount.getAndIncrement() == 0) {
                throw new RuntimeException('boom first tick')
            }
        })

        and:
        def eventloop = Eventloop.builder()
                .withThreadName('test-primary-reschedule-after-throw')
                .withIdleInterval(Duration.ofMillis(10))
                .build()
        eventloop.keepAlive(true)
        primaryTaskRunnable.primaryEventloop = eventloop

        when:
        Thread.start {
            Thread.currentThread().sleep(1000 * 2)
            primaryTaskRunnable.stop()
            Thread.currentThread().sleep(500)
            eventloop.breakEventloop()
        }
        // first run() tick throws inside the callback; the loop must catch it and keep rescheduling
        primaryTaskRunnable.run()
        eventloop.run()

        then:
        // first tick threw; if the loop kept rescheduling (1s interval), the callback ran again
        callCount.get() > 1
    }
}
