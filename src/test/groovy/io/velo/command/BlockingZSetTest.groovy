package io.velo.command

import io.activej.eventloop.Eventloop
import io.activej.promise.SettablePromise
import io.velo.BaseCommand
import io.velo.SocketInspectorTest
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.reply.BulkReply
import io.velo.reply.MultiBulkReply
import io.velo.reply.NilReply
import io.velo.reply.Reply
import io.velo.type.RedisZSet
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class BlockingZSetTest extends Specification {
    def 'test blocking client count and key count safe from non-slot-worker thread'() {
        given:
        def slotWorker1 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        def slotWorker2 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        slotWorker1.keepAlive(true)
        slotWorker2.keepAlive(true)
        Thread.start { slotWorker1.run() }
        Thread.start { slotWorker2.run() }
        Thread.sleep(500)

        Eventloop[] eventloopArray = [slotWorker1, slotWorker2]
        BlockingZSet.initBySlotWorkerEventloopArray(eventloopArray)

        when:
        def errors = Collections.synchronizedList(new ArrayList<Throwable>())
        def done = new CountDownLatch(4)

        // Slot worker 1: continuously add/remove
        slotWorker1.execute {
            try {
                List<BlockingZSet.PromiseWithMinOrMaxAndCreatedTime> added = []
                200.times {
                    def p = new SettablePromise<Reply>()
                    def one = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(p, null, true, System.currentTimeMillis(), 0)
                    BlockingZSet.addOne('key_a', one)
                    added << one
                    if (added.size() > 10) {
                        BlockingZSet.removeOne('key_a', added.remove(0))
                    }
                }
                added.each { BlockingZSet.removeOne('key_a', it) }
            } catch (Throwable t) {
                errors << t
            } finally {
                done.countDown()
            }
        }

        // Slot worker 2: continuously add/remove
        slotWorker2.execute {
            try {
                List<BlockingZSet.PromiseWithMinOrMaxAndCreatedTime> added = []
                200.times {
                    def p = new SettablePromise<Reply>()
                    def one = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(p, null, false, System.currentTimeMillis(), 2)
                    BlockingZSet.addOne('key_b', one)
                    added << one
                    if (added.size() > 10) {
                        BlockingZSet.removeOne('key_b', added.remove(0))
                    }
                }
                added.each { BlockingZSet.removeOne('key_b', it) }
            } catch (Throwable t) {
                errors << t
            } finally {
                done.countDown()
            }
        }

        // Reader thread 1: read blockingClientCount while workers mutate
        Thread.start {
            try {
                500.times {
                    def c = BlockingZSet.blockingClientCount()
                    assert c >= 0
                }
            } catch (Throwable t) {
                errors << t
            } finally {
                done.countDown()
            }
        }

        // Reader thread 2: read blockingKeyCount while workers mutate
        Thread.start {
            try {
                500.times {
                    def c = BlockingZSet.blockingKeyCount()
                    assert c >= 0
                }
            } catch (Throwable t) {
                errors << t
            } finally {
                done.countDown()
            }
        }

        done.await(10, TimeUnit.SECONDS)

        then:
        errors.isEmpty()

        cleanup:
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        slotWorker1.breakEventloop()
        slotWorker2.breakEventloop()
    }

    def 'test blocking zset promise add remove and remove by socket'() {
        given:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Thread.sleep(100)
        Eventloop[] eventloopArray = [eventloopCurrent]
        BlockingZSet.initBySlotWorkerEventloopArray(eventloopArray)

        final short slot = 0
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def inMemoryGetSet = new InMemoryGetSet()
        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when: 'accessors carry the waiter configuration'
        def settablePromise = new SettablePromise<Reply>()
        def one = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(settablePromise, null, false, 123L, 2)
        then:
        one.settablePromise() == settablePromise
        one.socket() == null
        !one.isMin()
        one.createdTime() == 123L
        one.mpopCount() == 2

        when:
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        BlockingZSet.addOne('a', one)
        then:
        BlockingZSet.blockingClientCount() == 1
        BlockingZSet.blockingKeyCount() == 1

        when:
        BlockingZSet.removeOne('a', one)
        then:
        BlockingZSet.blockingClientCount() == 0
        BlockingZSet.blockingKeyCount() == 0

        when: 'removing an unknown key or an already removed waiter is a no-op'
        BlockingZSet.removeOne('xx', one)
        BlockingZSet.removeOne('a', one)
        then:
        BlockingZSet.blockingClientCount() == 0

        // test socket disconnect
        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        def socket2 = SocketInspectorTest.mockTcpSocket(null, 46380)
        def settablePromise9 = new SettablePromise<Reply>()
        def settablePromise10 = new SettablePromise<Reply>()
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def oneWithSocket = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(settablePromise9, socket, true, System.currentTimeMillis(), 0)
        def oneWithSocket2 = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(settablePromise10, socket2, true, System.currentTimeMillis(), 0)
        BlockingZSet.addOne('yy', oneWithSocket)
        BlockingZSet.addOne('yy', oneWithSocket2)
        then:
        BlockingZSet.blockingKeyCount() == 1
        BlockingZSet.blockingClientCount() == 2

        when:
        BlockingZSet.removeBySocket(socket)
        eventloopCurrent.run()
        then:
        BlockingZSet.blockingClientCount() == 1

        when:
        BlockingZSet.removeBySocket(socket2)
        eventloopCurrent.run()
        then:
        BlockingZSet.blockingClientCount() == 0

        when: 'one waiter registered on two keys counts as one blocked client'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def oneOnTwoKeys = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(new SettablePromise<Reply>(), null, true,
                System.currentTimeMillis(), 0)
        BlockingZSet.addOne('two-keys-a', oneOnTwoKeys)
        BlockingZSet.addOne('two-keys-b', oneOnTwoKeys)
        then:
        BlockingZSet.blockingClientCount() == 1
        BlockingZSet.blockingKeyCount() == 2

        when: 'the waiter is released from one key, the client is still blocked on the other one'
        BlockingZSet.removeOne('two-keys-a', oneOnTwoKeys)
        then:
        BlockingZSet.blockingClientCount() == 1
        BlockingZSet.blockingKeyCount() == 1

        when: 'the waiter is released from the last key'
        BlockingZSet.removeOne('two-keys-b', oneOnTwoKeys)
        then:
        BlockingZSet.blockingClientCount() == 0
        BlockingZSet.blockingKeyCount() == 0

        cleanup:
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        localPersist.cleanUp()
    }

    def 'test on zset saved wakes the waiters'() {
        given:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Thread.sleep(100)
        Eventloop[] eventloopArray = [eventloopCurrent]
        BlockingZSet.initBySlotWorkerEventloopArray(eventloopArray)

        final short slot = 0
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def inMemoryGetSet = new InMemoryGetSet()
        def zGroup = new ZGroup(null, null, null)
        zGroup.byPassGetSet = inMemoryGetSet
        zGroup.from(BaseCommand.mockAGroup())

        when: 'a save without waiters stores the whole zset and notifies nobody'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def slotA = BaseCommand.slot('a', (short) 1)
        def rzA = new RedisZSet()
        rzA.add(1.0, 'one')
        rzA.add(2.0, 'two')
        ZGroup.saveRedisZSet(rzA, slotA, zGroup, zGroup.dictMap)
        then:
        BlockingZSet.blockingClientCount() == 0
        def storedA = ZGroup.getRedisZSet(slotA, zGroup)
        storedA.size() == 2

        when: 'a save wakes a single member waiter with the lowest score'
        def promise1 = new SettablePromise<Reply>()
        def one1 = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(promise1, null, true, System.currentTimeMillis(), 0)
        BlockingZSet.addOne('b', one1)
        def slotB = BaseCommand.slot('b', (short) 1)
        def rzB = new RedisZSet()
        rzB.add(1.5, 'low')
        rzB.add(9.9, 'high')
        ZGroup.saveRedisZSet(rzB, slotB, zGroup, zGroup.dictMap)
        then:
        promise1.isComplete()
        def result1 = promise1.getResult() as MultiBulkReply
        result1.replies.length == 3
        (result1.replies[0] as BulkReply).asString() == 'b'
        (result1.replies[1] as BulkReply).asString() == 'low'
        (result1.replies[2] as BulkReply).asString() == '1.5'
        BlockingZSet.blockingClientCount() == 0
        // the member the waiter popped is gone, the other one stays
        def storedB = ZGroup.getRedisZSet(slotB, zGroup)
        storedB.size() == 1
        storedB.contains('high')

        when: 'a max waiter gets the highest score'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def promise2 = new SettablePromise<Reply>()
        def one2 = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(promise2, null, false, System.currentTimeMillis(), 0)
        BlockingZSet.addOne('c', one2)
        def slotC = BaseCommand.slot('c', (short) 1)
        def rzC = new RedisZSet()
        rzC.add(1.0, 'a')
        rzC.add(26.0, 'z')
        ZGroup.saveRedisZSet(rzC, slotC, zGroup, zGroup.dictMap)
        then:
        promise2.isComplete()
        ((promise2.getResult() as MultiBulkReply).replies[1] as BulkReply).asString() == 'z'
        ((promise2.getResult() as MultiBulkReply).replies[2] as BulkReply).asString() == '26'
        ZGroup.getRedisZSet(slotC, zGroup).size() == 1

        when: 'an mpop waiter pops up to mpopCount members and the rest stays stored'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def promise3 = new SettablePromise<Reply>()
        def one3 = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(promise3, null, true, System.currentTimeMillis(), 2)
        BlockingZSet.addOne('d', one3)
        def slotD = BaseCommand.slot('d', (short) 1)
        def rzD = new RedisZSet()
        rzD.add(1.0, 'first')
        rzD.add(2.0, 'second')
        rzD.add(3.0, 'third')
        ZGroup.saveRedisZSet(rzD, slotD, zGroup, zGroup.dictMap)
        then:
        promise3.isComplete()
        def result3 = promise3.getResult() as MultiBulkReply
        result3.replies.length == 2
        (result3.replies[0] as BulkReply).asString() == 'd'
        def inner3 = result3.replies[1] as MultiBulkReply
        inner3.replies.length == 2
        def firstPair = inner3.replies[0] as MultiBulkReply
        firstPair.replies.length == 2
        (firstPair.replies[0] as BulkReply).asString() == 'first'
        (firstPair.replies[1] as BulkReply).asString() == '1'
        def secondPair = inner3.replies[1] as MultiBulkReply
        secondPair.replies.length == 2
        (secondPair.replies[0] as BulkReply).asString() == 'second'
        (secondPair.replies[1] as BulkReply).asString() == '2'
        def storedD = ZGroup.getRedisZSet(slotD, zGroup)
        storedD.size() == 1
        storedD.contains('third')

        when: 'one save with a single member completes only the first waiter'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def promise4 = new SettablePromise<Reply>()
        def promise5 = new SettablePromise<Reply>()
        def one4 = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(promise4, null, true, System.currentTimeMillis(), 0)
        def one5 = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(promise5, null, true, System.currentTimeMillis(), 0)
        BlockingZSet.addOne('e', one4)
        BlockingZSet.addOne('e', one5)
        def slotE = BaseCommand.slot('e', (short) 1)
        def rzE = new RedisZSet()
        rzE.add(7.7, 'only')
        ZGroup.saveRedisZSet(rzE, slotE, zGroup, zGroup.dictMap)
        then:
        promise4.isComplete()
        !promise5.isComplete()
        ((promise4.getResult() as MultiBulkReply).replies[1] as BulkReply).asString() == 'only'
        // the zset was drained by the first waiter
        ZGroup.getRedisZSet(slotE, zGroup) == null

        when: 'a stale waiter that already lost the delivery race is skipped and removed'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def stalePromise = new SettablePromise<Reply>()
        def staleOne = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(stalePromise, null, true, System.currentTimeMillis(), 0)
        staleOne.tryMarkDelivered()
        BlockingZSet.addOne('f', staleOne)
        def slotF = BaseCommand.slot('f', (short) 1)
        def rzF = new RedisZSet()
        rzF.add(1.0, 'keep')
        ZGroup.saveRedisZSet(rzF, slotF, zGroup, zGroup.dictMap)
        then:
        !stalePromise.isComplete()
        BlockingZSet.blockingClientCount() == 0
        // nobody popped, the member stays stored
        ZGroup.getRedisZSet(slotF, zGroup).size() == 1

        when: 'saving an empty zset removes the key and never wakes anybody'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def promise6 = new SettablePromise<Reply>()
        def one6 = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(promise6, null, true, System.currentTimeMillis(), 0)
        BlockingZSet.addOne('g', one6)
        def slotG = BaseCommand.slot('g', (short) 1)
        ZGroup.saveRedisZSet(new RedisZSet(), slotG, zGroup, zGroup.dictMap)
        then:
        !promise6.isComplete()
        ZGroup.getRedisZSet(slotG, zGroup) == null

        when: 'a waiter whose promise is already complete is dropped as stale'
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        def donePromise = new SettablePromise<Reply>()
        donePromise.set(NilReply.INSTANCE)
        def doneOne = new BlockingZSet.PromiseWithMinOrMaxAndCreatedTime(donePromise, null, true, System.currentTimeMillis(), 0)
        BlockingZSet.addOne('h', doneOne)
        def slotH = BaseCommand.slot('h', (short) 1)
        def rzH = new RedisZSet()
        rzH.add(1.0, 'stays')
        ZGroup.saveRedisZSet(rzH, slotH, zGroup, zGroup.dictMap)
        then:
        BlockingZSet.blockingClientCount() == 0
        ZGroup.getRedisZSet(slotH, zGroup).size() == 1

        cleanup:
        BlockingZSet.clearBlockingZSetPromisesForAllKeys()
        localPersist.cleanUp()
    }
}
