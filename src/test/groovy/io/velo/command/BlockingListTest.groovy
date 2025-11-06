package io.velo.command

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

class BlockingListTest extends Specification {
    def 'test blocking list promise'() {
        expect:
        !BlockingList.setReplyIfBlockingListExist('a', new byte[1][0])
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
        def bb = BlockingList.setReplyIfBlockingListExist('a', elementValueBytesArray)
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
        def bb2 = BlockingList.setReplyIfBlockingListExist('a', elementValueBytesArray2)
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
        def bb33 = BlockingList.setReplyIfBlockingListExist('a', elementValueBytesArray33)
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
        def bb333 = BlockingList.setReplyIfBlockingListExist('a', elementValueBytesArray333)
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
        def bb3333 = BlockingList.setReplyIfBlockingListExist('a', elementValueBytesArray3333)
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
        def bb5 = BlockingList.setReplyIfBlockingListExist('a', elementValueBytesArray5)
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

        def slotForKeyB = BaseCommand.slot('b'.bytes, (short) 1)
        def xx = new BlockingList.DstKeyAndDstLeftWhenMove('b'.bytes, slotForKeyB, true)

        BlockingList.clearBlockingListPromisesForAllKeys()
        def settablePromise7 = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise7, null, true, xx)
        def elementValueBytesArray7 = new byte[1][]
        elementValueBytesArray7[0] = 'a'.bytes
        def bb7 = BlockingList.setReplyIfBlockingListExist('a', elementValueBytesArray7, bGroup)
        then:
        settablePromise7.isComplete()
        (settablePromise7.getResult() as BulkReply).raw == 'a'.bytes
        bb7.length == 0
        inMemoryGetSet.getBuf(slot, 'b'.bytes, slotForKeyB.bucketIndex(), slotForKeyB.keyHash()) != null

        when:
        BlockingList.clearBlockingListPromisesForAllKeys()
        def settablePromise8 = new SettablePromise<Reply>()
        BlockingList.addBlockingListPromiseByKey('a', settablePromise8, null, false, xx)
        def elementValueBytesArray8 = new byte[1][]
        elementValueBytesArray8[0] = 'a'.bytes
        def bb8 = BlockingList.setReplyIfBlockingListExist('a', elementValueBytesArray8, bGroup)
        then:
        settablePromise8.isComplete()
        (settablePromise8.getResult() as BulkReply).raw == 'a'.bytes
        bb8.length == 0
        inMemoryGetSet.getBuf(slot, 'b'.bytes, slotForKeyB.bucketIndex(), slotForKeyB.keyHash()) != null

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
        then:
        BlockingList.blockingClientCount() == 1

        when:
        BlockingList.removeBySocket(socket2)
        then:
        BlockingList.blockingClientCount() == 0

        cleanup:
        localPersist.cleanUp()
    }
}
