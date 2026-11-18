package io.velo.reply

import spock.lang.Specification

class MultiFrameReplyTest extends Specification {
    def 'test multi frame reply'() {
        given:
        // like the pubsub confirmation frames: one 3-element array per channel
        def tripleA = new MultiBulkReply([new BulkReply('unsubscribe'), new BulkReply('a'), new IntegerReply(1)] as Reply[])
        def tripleB = new MultiBulkReply([new BulkReply('unsubscribe'), new BulkReply('b'), new IntegerReply(0)] as Reply[])
        def reply = new MultiFrameReply(tripleA, tripleB)

        expect:
        reply.frames.length == 2
        // each frame keeps its own complete RESP encoding, written back to back
        reply.buffer().asArray() == ('*3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n' +
                '*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n').getBytes()
        // resp3 delegates per frame: BulkReply renders as a resp3 simple string
        reply.bufferAsResp3().asArray() == ('*3\r\n+unsubscribe\r\n+a\r\n:1\r\n' +
                '*3\r\n+unsubscribe\r\n+b\r\n:0\r\n').getBytes()
        new String(reply.bufferAsHttp().asArray()) == '*3\n$11\nunsubscribe\n$1\na\n:1\n*3\n$11\nunsubscribe\n$1\nb\n:0\n'
        reply.dumpForTest(new StringBuilder(), 0)
    }

    def 'test single frame and empty'() {
        given:
        def single = new MultiFrameReply(new IntegerReply(7))
        def empty = new MultiFrameReply()

        expect:
        single.frames.length == 1
        single.buffer().asArray() == ':7\r\n'.getBytes()

        and:
        empty.frames.length == 0
        empty.buffer().asArray().length == 0
    }
}
