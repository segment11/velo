package io.velo.task

import io.activej.eventloop.Eventloop
import io.velo.persist.Consts
import io.velo.persist.OneSlot
import spock.lang.Specification

import java.time.Duration

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
}
