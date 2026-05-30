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
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

class BlockingListTest extends Specification {
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
        BlockingList.initBySlotWorkerEventloopArray(eventloopArray)

        when:
        def errors = Collections.synchronizedList(new ArrayList<Throwable>())
        def done = new CountDownLatch(4)

        // Slot worker 1: continuously add/remove
        slotWorker1.execute {
            try {
                List<BlockingList.PromiseWithLeftOrRightAndCreatedTime> added = []
                200.times {
                    def p = new SettablePromise<Reply>()
                    def one = BlockingList.addBlockingListPromiseByKey('key_a', p, null, true)
                    added << one
                    if (added.size() > 10) {
                        BlockingList.removeBlockingListPromiseByKey('key_a', added.remove(0))
                    }
                }
                added.each { BlockingList.removeBlockingListPromiseByKey('key_a', it) }
            } catch (Throwable t) {
                errors << t
            } finally {
                done.countDown()
            }
        }

        // Slot worker 2: continuously add/remove
        slotWorker2.execute {
            try {
                List<BlockingList.PromiseWithLeftOrRightAndCreatedTime> added = []
                200.times {
                    def p = new SettablePromise<Reply>()
                    def one = BlockingList.addBlockingListPromiseByKey('key_b', p, null, true)
                    added << one
                    if (added.size() > 10) {
                        BlockingList.removeBlockingListPromiseByKey('key_b', added.remove(0))
                    }
                }
                added.each { BlockingList.removeBlockingListPromiseByKey('key_b', it) }
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
                    def c = BlockingList.blockingClientCount()
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
                    def c = BlockingList.blockingKeyCount()
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
        BlockingList.clearBlockingListPromisesForAllKeys()
        slotWorker1.breakEventloop()
        slotWorker2.breakEventloop()
    }

    def 'test blocking list promise'() {
        given:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Thread.sleep(100)
        Eventloop[] eventloopArray = [eventloopCurrent]
        BlockingList.initBySlotWorkerEventloopArray(eventloopArray)

        expect:
        !BlockingList.setReplyRPushIfBlockingListExist('a', new byte[1][0])
        BlockingList.blockingClientCount() == 0

        when:
        def settablePromise = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise, null, true)
        then:
        BlockingList.blockingClientCount() == 1
        BlockingList.blockingKeyCount() == 1

        when:
        def elementValueBytesArray = new byte[1][]
        elementValueBytesArray[0] = 'a'.bytes
        def bb = BlockingList.setReplyRPushIfBlockingListExist('a', elementValueBytesArray)
        then:
        settablePromise.isComplete()
        settablePromise.getResult() instanceof MultiBulkReply
        ((settablePromise.getResult() as MultiBulkReply).replies[1] as BulkReply).raw == 'a'.bytes
        bb.length == 0

        when:
        def settablePromise2 = new SettablePromise<Reply>()
        def settablePromise3 = new SettablePromise<Reply>()
        def settablePromise4 = new SettablePromise<Reply>()
        settablePromise2.set(NilReply.INSTANCE)
        BlockingList.addBlockingListPromiseByKey('a', settablePromise2, null, true)
        BlockingList.addBlockingListPromiseByKey('a', settablePromise3, null, false)
        BlockingList.addBlockingListPromiseByKey('a', settablePromise4, null, true)
        def elementValueBytesArray2 = new byte[1][]
        elementValueBytesArray2[0] = 'a'.bytes
        def bb2 = BlockingList.setReplyRPushIfBlockingListExist('a', elementValueBytesArray2)
        then:
        settablePromise2.isComplete()
        settablePromise3.isComplete()
        !settablePromise4.isComplete()
        bb2.length == 0

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        def settablePromise33 = new SettablePromise<Reply>()
        def settablePromise44 = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise44, null, true)
        BlockingList.addBlockingListPromiseByKey('a', settablePromise33, null, false)
        def elementValueBytesArray33 = new byte[1][]
        elementValueBytesArray33[0] = 'a'.bytes
        def bb33 = BlockingList.setReplyRPushIfBlockingListExist('a', elementValueBytesArray33)
        then:
        settablePromise44.isComplete()
        !settablePromise33.isComplete()
        bb33.length == 0

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        def settablePromise333 = new SettablePromise<Reply>()
        def settablePromise444 = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise333, null, true)
        BlockingList.addBlockingListPromiseByKey('a', settablePromise444, null, true)
        def elementValueBytesArray333 = new byte[1][]
        elementValueBytesArray333[0] = 'a'.bytes
        def bb333 = BlockingList.setReplyRPushIfBlockingListExist('a', elementValueBytesArray333)
        then:
        settablePromise333.isComplete()
        !settablePromise444.isComplete()
        bb333.length == 0

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        def settablePromise3333 = new SettablePromise<Reply>()
        def settablePromise4444 = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise3333, null, false)
        BlockingList.addBlockingListPromiseByKey('a', settablePromise4444, null, false)
        def elementValueBytesArray3333 = new byte[1][]
        elementValueBytesArray3333[0] = 'a'.bytes
        def bb3333 = BlockingList.setReplyLPushIfBlockingListExist('a', elementValueBytesArray3333)
        then:
        settablePromise3333.isComplete()
        !settablePromise4444.isComplete()
        bb3333.length == 0

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        def settablePromise5 = new SettablePromise<Reply>()
        def settablePromise6 = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise5, null, true)
        BlockingList.addBlockingListPromiseByKey('a', settablePromise6, null, false)
        def elementValueBytesArray5 = new byte[3][]
        elementValueBytesArray5[0] = 'a'.bytes
        elementValueBytesArray5[1] = 'b'.bytes
        elementValueBytesArray5[2] = 'c'.bytes
        def bb5 = BlockingList.setReplyRPushIfBlockingListExist('a', elementValueBytesArray5)
        then:
        settablePromise5.isComplete()
        ((settablePromise5.getResult() as MultiBulkReply).replies[1] as BulkReply).raw == 'a'.bytes
        settablePromise6.isComplete()
        ((settablePromise6.getResult() as MultiBulkReply).replies[1] as BulkReply).raw == 'c'.bytes
        bb5.length == 1

        when:
        final short slot = 0
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def inMemoryGetSet = new InMemoryGetSet()

        def bGroup = new BGroup(null, null, null)
        bGroup.byPassGetSet = inMemoryGetSet
        bGroup.from(BaseCommand.mockAGroup())

        def slotForKeyB = BaseCommand.slot('b', (short) 1)
        def xx = new BlockingList.DstKeyAndDstLeftWhenMove(slotForKeyB, true)

        BlockingList.clearBlockingListPromisesForAllKeys()
        def settablePromise7 = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise7, null, true, xx)
        def elementValueBytesArray7 = new byte[1][]
        elementValueBytesArray7[0] = 'a'.bytes
        def bb7 = BlockingList.setReplyIfBlockingListExist('a', true, elementValueBytesArray7, bGroup)
        then:
        settablePromise7.isComplete()
        (settablePromise7.getResult() as BulkReply).raw == 'a'.bytes
        bb7.length == 0
        inMemoryGetSet.getBuf(slot, 'b', slotForKeyB.bucketIndex(), slotForKeyB.keyHash()) != null

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        def settablePromise8 = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise8, null, false, xx)
        def elementValueBytesArray8 = new byte[3][]
        elementValueBytesArray8[0] = 'a'.bytes
        elementValueBytesArray8[1] = 'b'.bytes
        elementValueBytesArray8[2] = 'c'.bytes
        def bb8 = BlockingList.setReplyIfBlockingListExist('a', true, elementValueBytesArray8, bGroup)
        then:
        settablePromise8.isComplete()
        (settablePromise8.getResult() as BulkReply).raw == 'a'.bytes
        bb8.length == 2

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        def settablePromise88 = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise88, null, true, xx)
        def elementValueBytesArray88 = new byte[3][]
        elementValueBytesArray88[0] = 'a'.bytes
        elementValueBytesArray88[1] = 'b'.bytes
        elementValueBytesArray88[2] = 'c'.bytes
        def bb88 = BlockingList.setReplyIfBlockingListExist('a', false, elementValueBytesArray88, bGroup)
        then:
        settablePromise88.isComplete()
        (settablePromise88.getResult() as BulkReply).raw == 'a'.bytes
        bb88.length == 2

        when:
        def one = BlockingList.addBlockingListPromiseByKey('a', settablePromise8, null, true)
        BlockingList.removeBlockingListPromiseByKey('a', one)
        BlockingList.removeBlockingListPromiseByKey('xx', one)
        then:
        1 == 1

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        BlockingList.addOne('a', one)
        then:
        BlockingList.blockingClientCount() == 1

        when:
        BlockingList.removeOne('a', one)
        then:
        BlockingList.blockingClientCount() == 0

        // test socket disconnect
        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        def socket2 = SocketInspectorTest.mockTcpSocket(null, 46380)
        def settablePromise9 = new SettablePromise<Reply>()
        def settablePromise10 = new SettablePromise<Reply>()
        BlockingList.clearBlockingListPromisesForAllKeys()
        BlockingList.addBlockingListPromiseByKey('yy', settablePromise9, socket, true)
        BlockingList.addBlockingListPromiseByKey('yy', settablePromise10, socket2, true)
        then:
        BlockingList.blockingKeyCount() == 1
        BlockingList.blockingClientCount() == 2

        when:
        BlockingList.removeBySocket(socket)
        eventloopCurrent.run()
        then:
        BlockingList.blockingClientCount() == 1

        when:
        BlockingList.removeBySocket(socket2)
        eventloopCurrent.run()
        then:
        BlockingList.blockingClientCount() == 0

        cleanup:
        localPersist.cleanUp()
    }

    def 'test blmpop wake-up returns nested array'() {
        given:
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Thread.sleep(100)
        Eventloop[] eventloopArray = [eventloopCurrent]
        BlockingList.initBySlotWorkerEventloopArray(eventloopArray)

        when: 'BLMPOP COUNT 1 wake-up returns [key, [value]]'
        BlockingList.clearBlockingListPromisesForAllKeys()
        def promise1 = new SettablePromise<Reply>()
        def one1 = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(promise1, null, true, System.currentTimeMillis(), null, 1)
        BlockingList.addOne('mylist', one1)
        def elements1 = new byte[1][]
        elements1[0] = 'hello'.bytes
        BlockingList.setReplyRPushIfBlockingListExist('mylist', elements1)
        then:
        promise1.isComplete()
        def result1 = promise1.getResult() as MultiBulkReply
        result1.replies.length == 2
        (result1.replies[0] as BulkReply).raw == 'mylist'.bytes
        result1.replies[1] instanceof MultiBulkReply
        def inner1 = result1.replies[1] as MultiBulkReply
        inner1.replies.length == 1
        (inner1.replies[0] as BulkReply).raw == 'hello'.bytes

        when: 'BLMPOP COUNT 2 wake-up collects 2 elements'
        BlockingList.clearBlockingListPromisesForAllKeys()
        def promise2 = new SettablePromise<Reply>()
        def one2 = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(promise2, null, true, System.currentTimeMillis(), null, 2)
        BlockingList.addOne('mylist2', one2)
        def elements2 = new byte[3][]
        elements2[0] = 'a'.bytes
        elements2[1] = 'b'.bytes
        elements2[2] = 'c'.bytes
        def remaining = BlockingList.setReplyRPushIfBlockingListExist('mylist2', elements2)
        then:
        promise2.isComplete()
        def result2 = promise2.getResult() as MultiBulkReply
        result2.replies.length == 2
        (result2.replies[0] as BulkReply).raw == 'mylist2'.bytes
        def inner2 = result2.replies[1] as MultiBulkReply
        inner2.replies.length == 2
        (inner2.replies[0] as BulkReply).raw == 'a'.bytes
        (inner2.replies[1] as BulkReply).raw == 'b'.bytes
        // 'c' was not consumed
        remaining.length == 1

        when: 'BLMPOP COUNT 3 but only 2 elements available'
        BlockingList.clearBlockingListPromisesForAllKeys()
        def promise3 = new SettablePromise<Reply>()
        def one3 = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(promise3, null, true, System.currentTimeMillis(), null, 3)
        BlockingList.addOne('mylist3', one3)
        def elements3 = new byte[2][]
        elements3[0] = 'x'.bytes
        elements3[1] = 'y'.bytes
        BlockingList.setReplyRPushIfBlockingListExist('mylist3', elements3)
        then:
        promise3.isComplete()
        def result3 = promise3.getResult() as MultiBulkReply
        def inner3 = result3.replies[1] as MultiBulkReply
        inner3.replies.length == 2
        (inner3.replies[0] as BulkReply).raw == 'x'.bytes
        (inner3.replies[1] as BulkReply).raw == 'y'.bytes

        when: 'right pop with COUNT 2'
        BlockingList.clearBlockingListPromisesForAllKeys()
        def promise4 = new SettablePromise<Reply>()
        def one4 = new BlockingList.PromiseWithLeftOrRightAndCreatedTime(promise4, null, false, System.currentTimeMillis(), null, 2)
        BlockingList.addOne('mylist4', one4)
        def elements4 = new byte[3][]
        elements4[0] = 'p'.bytes
        elements4[1] = 'q'.bytes
        elements4[2] = 'r'.bytes
        BlockingList.setReplyRPushIfBlockingListExist('mylist4', elements4)
        then:
        promise4.isComplete()
        def result4 = promise4.getResult() as MultiBulkReply
        def inner4 = result4.replies[1] as MultiBulkReply
        inner4.replies.length == 2
        (inner4.replies[0] as BulkReply).raw == 'r'.bytes
        (inner4.replies[1] as BulkReply).raw == 'q'.bytes

        cleanup:
        BlockingList.clearBlockingListPromisesForAllKeys()
    }
}
