package io.velo.persist

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.velo.ConfForGlobal
import io.velo.RequestHandler
import io.velo.SnowFlake
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.reply.IntegerReply
import spock.lang.Specification

import java.time.Duration

class LocalPersistTest extends Specification {
    static void prepareLocalPersist(byte netWorkers = 1, short slotNumber = 1) {
        if (!ConfForGlobal.netListenAddresses) {
            ConfForGlobal.netListenAddresses = 'localhost:7379'
        }

        def localPersist = LocalPersist.instance

        RequestHandler.initMultiShardShadows(netWorkers)

        def snowFlakes = new SnowFlake[netWorkers]
        for (int i = 0; i < netWorkers; i++) {
            snowFlakes[i] = new SnowFlake(i + 1, 1)
        }
        localPersist.initSlots(netWorkers, slotNumber, snowFlakes, Consts.persistDir, Config.create())
        localPersist.debugMode()
    }

    final short slot = 0
    final short slot1 = 1

    def 'test all'() {
        given:
        def localPersist = LocalPersist.instance

        expect:
        LocalPersist.PAGE_SIZE == 4096
        LocalPersist.PROTECTION == 7
        LocalPersist.DEFAULT_SLOT_NUMBER == 4
        LocalPersist.MAX_SLOT_NUMBER == 1024
        localPersist.oneSlots() == null

        when:
        prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        then:
        localPersist.isDebugMode()
        localPersist.oneSlots().length == 1
        localPersist.oneSlot(slot) != null
        localPersist.currentThreadFirstOneSlot() == localPersist.oneSlots()[0]

        when:
        localPersist.startIndexHandlerPool()
        localPersist.asSlaveFirstSlotFetchedExistsAllDone = true
        then:
        localPersist.indexHandlerPool != null
        localPersist.asSlaveFirstSlotFetchedExistsAllDone

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
        localPersist.fixSlotThreadId(slot1, Thread.currentThread().threadId())
        then:
        localPersist.oneSlots().length == 2
        localPersist.currentThreadFirstOneSlot() == localPersist.oneSlots()[1]

        when:
        ConfForGlobal.clusterEnabled = false
        then:
        localPersist.firstOneSlot() == localPersist.oneSlots()[0]
        localPersist.lastOneSlot() == localPersist.oneSlots()[1]
        localPersist.nextOneSlot((short) 0) == localPersist.oneSlots()[1]

        when:
        ConfForGlobal.clusterEnabled = true
        ConfForGlobal.slotNumber = (short) 2
        localPersist.multiShard.shards << new Shard(nodes: [new Node(master: true, host: 'localhost', port: 7380)])
        then:
        localPersist.firstOneSlot() == null
        localPersist.lastOneSlot() == null
        localPersist.nextOneSlot((short) 0) == null

        when:
        localPersist.multiShard.shards[0].multiSlotRange.addSingle(8192, 16383)
        then:
        localPersist.firstOneSlot() == localPersist.oneSlots()[1]
        localPersist.lastOneSlot() == localPersist.oneSlots()[1]
        localPersist.nextOneSlot((short) 0) == localPersist.oneSlots()[1]

        when:
        boolean exception = false
        localPersist.fixSlotThreadId(slot1, -1L)
        try {
            localPersist.currentThreadFirstOneSlot()
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def isOkWalLazyRead = localPersist.walLazyReadFromFile()
        def isOkWarmUp = localPersist.warmUp()
        then:
        isOkWalLazyRead
        isOkWarmUp

        cleanup:
        ConfForGlobal.clusterEnabled = false
        ConfForGlobal.slotNumber = (short) 1
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId(slot1, Thread.currentThread().threadId())
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

        ConfForGlobal.netListenAddresses = 'localhost:7379'
        ConfForGlobal.initDynConfigItems.a = '97'
        RequestHandler.initMultiShardShadows(netWorkers)

        def snowFlakes = new SnowFlake[netWorkers]
        for (int i = 0; i < netWorkers; i++) {
            snowFlakes[i] = new SnowFlake(i + 1, 1)
        }

        def localPersist = LocalPersist.instance
        localPersist.initSlots(netWorkers, slotNumber, snowFlakes, Consts.persistDir,
                Config.create().with('isHashSaveMemberTogether', 'true'))
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        expect:
        localPersist.isHashSaveMemberTogether
        localPersist.multiShard != null

        when:
        localPersist.hashSaveMemberTogether = false
        then:
        !localPersist.isHashSaveMemberTogether

        when:
        def reply = localPersist.doSthInSlots(oneSlot -> {
            return 1L
        }, (ArrayList<Long> resultList) -> {
            def sum = resultList.sum() as long
            return new IntegerReply(sum)
        })
        then:
        reply.settablePromise.getResult() instanceof IntegerReply
        (reply.settablePromise.getResult() as IntegerReply).integer == 1L

        cleanup:
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.cleanUp()
    }
}
