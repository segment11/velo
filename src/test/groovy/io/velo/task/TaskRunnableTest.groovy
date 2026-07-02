package io.velo.task

import io.activej.eventloop.Eventloop
import io.velo.persist.Consts
import io.velo.persist.OneSlot
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

class TaskRunnableTest extends Specification {
    def 'test delay run task'() {
        given:
        final short slot = 0
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        oneSlot.taskChain.add(new TaskChainTest.Task1())

        OneSlot[] oneSlots = new OneSlot[1]
        oneSlots[0] = oneSlot

        and:
        def taskRunnable = new TaskRunnable((byte) 0, (byte) 1)
        taskRunnable.chargeOneSlots(oneSlots)
        taskRunnable.startDone(true)

        def eventloop = Eventloop.builder()
                .withThreadName('test-task-runnable')
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)

        taskRunnable.slotWorkerEventloop = eventloop
        taskRunnable.requestHandler = null

        when:
        Thread.start {
            Thread.currentThread().sleep(1000 * 2)
            taskRunnable.stop()
            Thread.currentThread().sleep(1000 * 2)
            eventloop.breakEventloop()
        }

        taskRunnable.run()
        eventloop.run()

        then:
        1 == 1
    }

    def 'test some branches'() {
        given:
        final short slot = 0
        def oneSlot = new OneSlot(slot, Consts.slotDir, null, null)
        oneSlot.taskChain.add(new TaskChainTest.Task1())
        def oneSlot2 = new OneSlot((byte) 1, Consts.slotDir, null, null)
        oneSlot2.taskChain.add(new TaskChainTest.Task1())


        OneSlot[] oneSlots = new OneSlot[2]
        oneSlots[0] = oneSlot
        oneSlots[1] = oneSlot2

        and:
        def taskRunnable = new TaskRunnable((byte) 0, (byte) 2)
        taskRunnable.chargeOneSlots(oneSlots)

        expect:
        taskRunnable.oneSlots.size() == 1

        when:
        taskRunnable.stop()
        taskRunnable.run()

        then:
        1 == 1
    }

    def 'test scheduler keeps running after a slot doTask throws'() {
        given:
        // a slot whose doTask throws on the first tick, then just counts subsequent ticks
        def callCount = new AtomicInteger()
        def oneSlot = new OneSlot((short) 0) {
            @Override
            void doTask(long loopCount) {
                if (callCount.getAndIncrement() == 0) {
                    throw new RuntimeException('boom first tick')
                }
            }
        }
        OneSlot[] oneSlots = [oneSlot] as OneSlot[]

        and:
        def taskRunnable = new TaskRunnable((byte) 0, (byte) 1)
        taskRunnable.chargeOneSlots(oneSlots)
        taskRunnable.startDone(true)

        def eventloop = Eventloop.builder()
                .withThreadName('test-task-reschedule-after-throw')
                .withIdleInterval(Duration.ofMillis(10))
                .build()
        eventloop.keepAlive(true)
        taskRunnable.slotWorkerEventloop = eventloop
        taskRunnable.requestHandler = null

        when:
        Thread.start {
            Thread.currentThread().sleep(1000)
            taskRunnable.stop()
            Thread.currentThread().sleep(500)
            eventloop.breakEventloop()
        }
        // first run() tick throws inside doTask; the loop must catch it and keep rescheduling
        taskRunnable.run()
        eventloop.run()

        then:
        // first tick threw; if the loop kept rescheduling, doTask ran many more times
        callCount.get() > 1
    }
}
