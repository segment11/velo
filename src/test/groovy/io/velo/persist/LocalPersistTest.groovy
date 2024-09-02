package io.velo.persist

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.velo.SnowFlake
import spock.lang.Specification

import java.time.Duration

class LocalPersistTest extends Specification {
    static void prepareLocalPersist(byte netWorkers = 1, short slotNumber = 1) {
        def localPersist = LocalPersist.instance

        def snowFlakes = new SnowFlake[netWorkers]
        for (int i = 0; i < netWorkers; i++) {
            snowFlakes[i] = new SnowFlake(i + 1, 1)
        }
        localPersist.initSlots(netWorkers, slotNumber, snowFlakes, Consts.persistDir, Config.create())
        localPersist.debugMode()
    }

    final short slot = 0

    def 'test all'() {
        given:
        def localPersist = LocalPersist.instance

        expect:
        LocalPersist.PAGE_SIZE == 4096
        LocalPersist.PROTECTION == 7
        LocalPersist.DEFAULT_SLOT_NUMBER == 4
        LocalPersist.MAX_SLOT_NUMBER == 1024
        LocalPersist.O_DIRECT == 040000
        localPersist.oneSlots() == null

        when:
        prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.persistMergedSegmentsJobUndone()
        then:
        localPersist.oneSlots().length == 1
        localPersist.oneSlot(slot) != null
        localPersist.currentThreadFirstOneSlot() == localPersist.oneSlots()[0]

        when:
        localPersist.startIndexHandlerPool()
        then:
        localPersist.indexHandlerPool != null

        cleanup:
        Thread.sleep(100)
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test multi slot'() {
        given:
        def localPersist = LocalPersist.instance

        when:
        prepareLocalPersist((byte) 1, (short) 2)
        localPersist.fixSlotThreadId((short) 1, Thread.currentThread().threadId())
        then:
        localPersist.oneSlots().length == 2
        localPersist.currentThreadFirstOneSlot() == localPersist.oneSlots()[1]

        when:
        boolean exception = false
        localPersist.fixSlotThreadId((short) 1, -1L)
        try {
            localPersist.currentThreadFirstOneSlot()
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId((short) 1, Thread.currentThread().threadId())
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test mock one slot'() {
        given:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }

        def localPersist = LocalPersist.instance
        localPersist.addOneSlot(slot, eventloop)
        localPersist.addOneSlotForTest2(slot)

        expect:
        localPersist.oneSlots().length == 1

        when:
        localPersist.socketInspector = null
        then:
        localPersist.socketInspector == null

        cleanup:
        eventloop.breakEventloop()
    }

    def 'test init slots'() {
        given:
        byte netWorkers = 1
        short slotNumber = 1
        def localPersist = LocalPersist.instance

        def snowFlakes = new SnowFlake[netWorkers]
        for (int i = 0; i < netWorkers; i++) {
            snowFlakes[i] = new SnowFlake(i + 1, 1)
        }
        localPersist.initSlots(netWorkers, slotNumber, snowFlakes, Consts.persistDir,
                Config.create().with('isHashSaveMemberTogether', 'true'))

        expect:
        localPersist.isHashSaveMemberTogether

        when:
        localPersist.hashSaveMemberTogether = false
        then:
        !localPersist.isHashSaveMemberTogether

        cleanup:
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.cleanUp()
    }
}
