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

    def 'test chargeOneSlots skips null entries'() {
        given:
        // a sparse OneSlot[] where slot 1 is null (uninitialized, as in cluster lazy init)
        def oneSlot = new OneSlot((short) 0, Consts.slotDir, null, null)
        OneSlot[] oneSlots = [oneSlot, null] as OneSlot[]

        and:
        def taskRunnable = new TaskRunnable((byte) 0, (byte) 1)

        when:
        taskRunnable.chargeOneSlots(oneSlots)

        then:
        taskRunnable.oneSlots.size() == 1
        taskRunnable.oneSlots.contains(oneSlot)
    }

    def 'test addOneSlotIfCharged routes to owning worker and is idempotent'() {        given:
        // slotWorkers=2: slot 0 -> worker 0, slot 1 -> worker 1, slot 3 -> worker 1
        def worker0 = new TaskRunnable((byte) 0, (byte) 2)
        def worker1 = new TaskRunnable((byte) 1, (byte) 2)
        def oneSlot0 = new OneSlot((short) 0, Consts.slotDir, null, null)
        def oneSlot1 = new OneSlot((short) 1, Consts.slotDir, null, null)

        when:
        // lazy registration of slot 1 (simulating setslot init from a different worker context)
        worker1.addOneSlotIfCharged(oneSlot1)
        // idempotent: same slot is not added twice
        worker1.addOneSlotIfCharged(oneSlot1)
        // slot 0 is not owned by worker 1, must be rejected
        worker1.addOneSlotIfCharged(oneSlot0)

        then:
        worker1.oneSlots.size() == 1
        worker1.oneSlots.contains(oneSlot1)
        !worker1.oneSlots.contains(oneSlot0)
        worker0.oneSlots.isEmpty()

        when:
        // concurrent mutation from another thread while iterating must not throw (CopyOnWriteArrayList)
        def t = Thread.start {
            worker1.addOneSlotIfCharged(new OneSlot((short) 3, Consts.slotDir, null, null))
        }
        def snapshot = []
        for (var s : worker1.oneSlots) {
            snapshot.add(s)
        }
        t.join()

        then:
        noExceptionThrown()
        worker1.oneSlots.size() == 2
    }
}
